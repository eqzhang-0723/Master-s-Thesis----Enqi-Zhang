# -*- coding: utf-8 -*-
"""
Created on Tue Dec 30 10:58:04 2025

@author: zeq
"""
import pandas as pd
import geopandas as gpd
import pandas as pd
from pyproj import CRS, Transformer
import rasterio
from scipy.spatial import cKDTree
import numpy as np
from shapely.geometry import Point
from shapely.ops import nearest_points
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling

################################################################################################ 8. 计算每个国家的成本距离，成本分配


################################################################################################
# Step1: 在 ARCGIS 中直接用最原始的 "D:\01 实践与科研\2023 气候变化 非洲\data\05 WFP-VAM crop prices\05 WFP crop price\02 working data 2000+\03_WFP_FAO_price_all_commodity_year\WFP_FAO_all_crop_price_address_inland_2017_2024.xlsx"
# 输入： WFP_FAO_all_crop_price_address_inland_2017_2024.xlsx + surface_border_road_land_slope.tif
# 输出： cost_allocation_initial_border_road_land_slope
# 并行因子设置为 0 
################################################################################################
# 带入  surface_border_road_land_slope.tif 进行成本分配

# 主要问题： 有的市场落入成本栅格之外 1） 落在水域 2） 落在国家边界（成本栅格为 nan 值的区域）





################################################################################################
# Step2: Python 识别遗漏点
# 输入： t_cost_allocation_initial_border_road_land_slope (cost_allocation_initial_border_road_land_slope 栅格数据的属性表)
# 输出： markets_2017_2024_omit_nan_new_test.xlsx
################################################################################################
# 1. 导入数据
data_market_origin = pd.read_excel(r"D:\01 实践与科研\2023 气候变化 非洲\data\05 WFP-VAM crop prices\05 WFP crop price test\02 working data\03_WFP_USAID_price_all_commodity_year\local nominal\WFP_USAID_maize_price_address_2021_2023.xlsx")

gdb_path = r"D:\01 实践与科研\2023 气候变化 非洲\data\14 land surface\python_arcgis_test\arcgis\land allocation.gdb"
table_name = "t_cost_allocation_initial_border_road_land_slope"
data_cost = gpd.read_file(gdb_path, layer=table_name)


# 2. 识别遗漏点
# 2.1 给 data_market_origin 生成0-1的编号
data_market_origin['FID'] = range(1, len(data_market_origin) + 1)

# 将 FID 列移到第一列
data_market_origin = data_market_origin[['FID'] + [col for col in data_market_origin.columns if col != 'FID']]

# 2.2 识别遗漏
# 找出 data_market_origin 中的 FID 不在 data_cost 的 Value 列中的行
p_missing_fid_df = data_market_origin[~data_market_origin['FID'].isin(data_cost['Value'])]

# 将结果保存为新的数据框 p_oimt
p_omit = p_missing_fid_df.copy()






# 3 处理遗漏点1： 将遗漏点（落在水域）移到最近的成本栅格中心
# 3.0 将点投影到投影坐标系
p_omit_0 = p_omit.copy()

# 读取投影坐标系信息
projection_path = r"D:\01 实践与科研\2023 气候变化 非洲\data\14 land surface\arcgis\working data\projection\Lambert_Azimuthal_Equal_Area_projection.prj"
with open(projection_path, 'r') as file:
    projection_wkt = file.read()

# 定义原始和目标坐标系
crs_gcs_wgs_1984 = CRS("EPSG:4326")  # WGS 84 地理坐标系
crs_projected = CRS.from_wkt(projection_wkt)  # 目标投影坐标系

# 创建坐标转换器
transformer = Transformer.from_crs(crs_gcs_wgs_1984, crs_projected, always_xy=True)

# 定义转换函数
def transform_coordinates(row):
    x, y = transformer.transform(row['Longitude'], row['Latitude'])
    return pd.Series([x, y])

# 应用转换函数，并将结果添加到新列
p_omit_0[['Projected_X', 'Projected_Y']] = p_omit_0.apply(transform_coordinates, axis=1)



# 3.1 读取栅格数据
p_omit_1 = p_omit_0.copy()

raster_path = r"D:/01 实践与科研/2023 气候变化 非洲/data/14 land surface/arcgis/working data/projection/surface_border_road_land_slope.tif"
gdb_path = r"D:/01 实践与科研/2023 气候变化 非洲/data/14 land surface/arcgis/africa_road_raster_projection_Pozi_test.gdb"

# 加载国家边界数据
africa_soc = gpd.read_file(gdb_path, layer="africa_soc_projection")


# 3.2 将 p_omit_1 转换为 GeoDataFrame，并使用点坐标创建几何列
p_omit_1['geometry'] = p_omit_1.apply(lambda row: Point(row['Projected_X'], row['Projected_Y']), axis=1)
p_omit_gdf = gpd.GeoDataFrame(p_omit_1, geometry='geometry', crs=africa_soc.crs)

# 执行空间连接，根据实际地理位置为 p_omit_gdf 匹配所在的国家
p_omit_gdf = gpd.sjoin(p_omit_gdf, africa_soc[['NAME', 'geometry']], how='left', predicate='within')
p_omit_gdf.rename(columns={'NAME': 'Country'}, inplace=True)



# 3.3 赋值 Country 为空的
# 提取 Address 中的逗号前部分并赋值给 Country 列为空的数据行
p_omit_gdf.loc[p_omit_gdf['Country'].isna(), 'Country'] = (
    p_omit_gdf['Address'].str.split(',', expand=True)[0].str.upper()
)

# 删除 index_right 列
p_omit_gdf = p_omit_gdf.drop(columns=['index_right'])



# 3.4 打开栅格文件并批量计算非 NaN 栅格的中心坐标
with rasterio.open(raster_path) as src:
    transform = src.transform  # 获取地理转换信息
    data = src.read(1)  # 读取第一个波段数据
    
    # 存储有效栅格中心的坐标
    grid_coords = []
    for row in range(src.height):
        for col in range(src.width):
            if not np.isnan(data[row, col]):  # 检查栅格值是否为 NaN
                # 计算栅格中心坐标
                x, y = transform * (col + 0.5, row + 0.5)
                grid_coords.append((x, y))

# 将所有栅格中心点转换为 GeoDataFrame 以批量进行空间连接
grid_gdf = gpd.GeoDataFrame(geometry=[Point(x, y) for x, y in grid_coords], crs=africa_soc.crs)

# 使用空间连接将栅格中心点与国家边界范围匹配
grid_with_country = gpd.sjoin(grid_gdf, africa_soc[['NAME', 'geometry']], how='left', predicate='within')
grid_with_country['Projected_X'] = grid_with_country.geometry.x
grid_with_country['Projected_Y'] = grid_with_country.geometry.y
grid_with_country = grid_with_country[['Projected_X', 'Projected_Y', 'NAME']].rename(columns={'NAME': 'Country'})



# 3.5 将每个国家的栅格中心坐标和 p_omit_gdf 的坐标分别存入字典，以便更快地按国家查找最近邻
# 重置索引确保索引是连续的
p_omit_gdf = p_omit_gdf.reset_index(drop=True)

# 初始化 new_coords 列表，长度与 p_omit_gdf 行数一致
new_coords = [None] * len(p_omit_gdf)

# 对每个国家的栅格中心分别构建 KDTree，并查找最近的栅格中心
for country, country_points in p_omit_gdf.groupby('Country'):
    # 筛选出该国家的栅格中心点
    country_grid_coords = grid_with_country[grid_with_country['Country'] == country][['Projected_X', 'Projected_Y']].values
    if len(country_grid_coords) == 0:
        # 如果该国家没有栅格中心点，则跳过，保持该国家的点为 None
        continue
    
    # 构建 KDTree 并查找最近邻
    tree = cKDTree(country_grid_coords)
    distances, indices = tree.query(country_points[['Projected_X', 'Projected_Y']])
    
    # 将最近的栅格中心点坐标填入 new_coords 列表的对应索引
    for idx, nearest_idx in zip(country_points.index, indices):
        new_coords[idx] = country_grid_coords[nearest_idx]

# 将新的坐标赋值回 p_omit_gdf，处理 None 值
p_omit_gdf['new_Projected_X'] = [coord[0] if coord is not None else np.nan for coord in new_coords]
p_omit_gdf['new_Projected_Y'] = [coord[1] if coord is not None else np.nan for coord in new_coords]




# 3.6 将 p_omit_gdf 投影回地理坐标系
p_omit_2 = p_omit_gdf.copy()

# 读取 .prj 文件内容
prj_path = r"D:\01 实践与科研\2023 气候变化 非洲\data\14 land surface\arcgis\working data\projection\Lambert_Azimuthal_Equal_Area_projection.prj"
with open(prj_path, 'r') as file:
    prj_wkt = file.read()

# 定义源投影坐标系和目标地理坐标系
source_projection = CRS.from_wkt(prj_wkt)  # 使用读取的 WKT 内容创建 CRS
target_projection = CRS("EPSG:4326")  # WGS 84 地理坐标系

# 创建转换器
transformer = Transformer.from_crs(source_projection, target_projection, always_xy=True)

# 定义转换函数
def project_to_geographic(row):
    lon, lat = transformer.transform(row['new_Projected_X'], row['new_Projected_Y'])
    return pd.Series([lon, lat])


# 应用转换函数，将投影坐标转换为地理坐标，并添加到新列中
p_omit_2[['new_Longitude', 'new_Latitude']] = p_omit_2.apply(project_to_geographic, axis=1)


# 交换 Longitude 和 Latitude 列的位置
columns = list(p_omit_2.columns)
lon_index = columns.index('Longitude')
lat_index = columns.index('Latitude')

columns[lon_index], columns[lat_index] = columns[lat_index], columns[lon_index]
p_omit_2 = p_omit_2[columns]



# 4 将遗漏点的新坐标+边界新坐标和原来的所有市场合并

p_omit_3= p_omit_2.copy()

# 保留 p_omit_2 中的指定列，并重命名
p_omit_3 = p_omit_3[['Address', 'new_Longitude', 'new_Latitude']].copy()
p_omit_3.rename(columns={'new_Longitude': 'Longitude', 'new_Latitude': 'Latitude'}, inplace=True)


# 创建 p_markets_new 为 data_market_origin 的副本
p_markets_new = data_market_origin.copy()

# 将 p_omit_3 与 p_markets_new 中的 Address 进行匹配替换
# 设置 Address 为索引，便于 update 操作
p_markets_new.set_index('Address', inplace=True)
p_omit_3.set_index('Address', inplace=True)

# 用 p_omit_3 替换掉 p_markets_new 中相同 Address 的行
p_markets_new.update(p_omit_3)

# 重置索引
p_markets_new.reset_index(inplace=True)



# 5 保存
# 将 p_markets_new 保存到第一个路径
p_markets_new.to_excel(r"D:\01 实践与科研\2023 气候变化 非洲\data\14 land surface\python_arcgis_test\working data\markets_2021_2023_test_new_sample.xlsx", index=False)

# 将 p_markets_new 保存到第二个路径
p_markets_new.to_excel(r"D:\01 实践与科研\2023 气候变化 非洲\data\05 WFP-VAM crop prices\05 WFP crop price test\02 working data\03_WFP_USAID_price_all_commodity_year\local nominal\WFP_USAID_maize_price_address_2021_2023_new_test_new_sample.xlsx", index=False)






################################################################################################
# Step3: ARCGIS中 用 markets_2017_2024_omit_nan_new_test 作为新的市场点要素，用 surface_border_road_land_slope_clip.tif 进行成本分配
# 输入： markets_2017_2024_omit_nan_new_test 点要素，surface_border_road_land_slope_clip.tif 
# 输出： cost_allocation_new_border_road_land_slope 栅格数据

# 注意：有的点离得太近，在同一个栅格内，因此比如有1374个市场，但是最后成本分配的有效结果有1366个，说明有8个市场离其他市场太近，无法参与单独的成本分配

# Step4: ARCGIS中 将成本分配的栅格结果转换为面，不要平滑
# 输入： cost_allocation_new_border_road_land_slope 栅格数据
# 输出： all_cost_result_with_nan_test.shp 面要素

# 注意：面要素中 gridcode （在栅格转面的时候，根据 Value 来转换）对应一开始 maret list 的排序

















































