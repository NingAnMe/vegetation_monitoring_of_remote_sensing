#!/usr/bin/env python
# -*- coding: utf-8 -*-
#add by yan on 2019-05-27

import os
import numpy as np
from osgeo import gdal
from osgeo import osr

def create_rgb(arrR, arrG, arrB, out_tiff_path, res=0.02, LonW=99.99, LatN=45.01):
    # --------------------------------------------
    # set geotransform
    ny, nx = arrR.shape
    xres = res
    yres = res
    geotransform = (LonW, xres, 0, LatN, 0, -yres)
#     print geotransform
#     print xmin, ymin, xmax, ymax, nx , ny
#     print np.min(lons), np.max(lats), np.max(lons), np.min(lats)
    dst_ds = gdal.GetDriverByName('GTiff').Create(
        out_tiff_path, nx, ny, 3, gdal.GDT_Byte, ["COMPRESS=LZW"])

    dst_ds.SetGeoTransform(geotransform)  # specify coords
    srs = osr.SpatialReference()  # establish encoding
    srs.ImportFromEPSG(4326)  # WGS84 lat/long
    dst_ds.SetProjection(srs.ExportToWkt())  # export coords to file
    dst_ds.GetRasterBand(1).WriteArray(arrR)  # write r-band to the raster
    # dst_ds.GetRasterBand(1).SetNoDataValue(1)  # set nodata value
    dst_ds.GetRasterBand(2).WriteArray(arrG)  # write g-band to the raster
    # dst_ds.GetRasterBand(2).SetNoDataValue(1)  # set nodata value
    dst_ds.GetRasterBand(3).WriteArray(arrB)  # write b-band to the raster
    # dst_ds.GetRasterBand(3).SetNoDataValue(1)  # set nodata valuer
    dst_ds.FlushCache()  # write to disk
    dst_ds = None

    out_path, file_name = os.path.split(out_tiff_path)
    out_jpg_path = os.path.join(out_path, file_name.split('.')[0] + '.jpg')
    dst_ds = gdal.GetDriverByName('MEM').Create(
        out_jpg_path, nx, ny, 3, gdal.GDT_Byte)
    dst_ds.GetRasterBand(3).WriteArray(arrB)  # write r-band to the raster
    dst_ds.GetRasterBand(2).WriteArray(arrG)  # write g-band to the raster
    dst_ds.GetRasterBand(1).WriteArray(arrR)  # write b-band to the raster

    png_dirver = gdal.GetDriverByName("JPEG")
    png_ds = png_dirver.CreateCopy(out_jpg_path, dst_ds)
    png_ds.FlushCache()  # write to disk
    dst_ds = None
    png_ds = None


def apply_lut_stretch(array_values, lut):
    # classification symbolization use color lookup table
    ny, nx = array_values.shape
    arrR = np.zeros_like(array_values)
    arrG = np.zeros_like(array_values)
    arrB = np.zeros_like(array_values)

    color_array = np.zeros((ny, nx, 3), np.uint8)

    print np.nanmin(array_values),np.nanmax(array_values)
    for i in range(0, ny):
        for j in range(0, nx):
            color_array[i, j] = lut[np.int(array_values[i, j])]

    arrR = color_array[:, :, 0]
    arrG = color_array[:, :, 1]
    arrB = color_array[:, :, 2]
    
    return arrR,arrG,arrB

def get_ndvi_data(out_data):
    ndvi_data=out_data['NDVI']
    ndvi_flag=out_data['Flag']

    print np.max(ndvi_data),np.min(ndvi_data)
    minRange = -0.2
    maxRange = 1.0
    # ndvi_data[np.where(ndvi_data>maxRange)]=maxRange
    # ndvi_data[np.where(ndvi_data<minRange)]=minRange
    minIdx=np.where(ndvi_data<minRange)
    maxIdx=np.where(ndvi_data>maxRange)
    ndvi_data[minIdx]=minRange
    ndvi_data[maxIdx]=maxRange

    # ndvi_data[np.where(ndvi_flag>0)]=np.nan
    print np.max(ndvi_data),np.min(ndvi_data)
    aryBand = np.floor((ndvi_data - minRange) / (maxRange - minRange) * 99)
    print np.max(ndvi_data),np.min(ndvi_data)
    ##make cloud,water and nan data as sepcific color
    ##cloud--255,255,255
    ##water--0,0,255
    ##nan--0,0,0
    cloud_mask=np.where(ndvi_flag==1)
    water_mask=np.where(ndvi_flag==2)
    nan_mask=np.where(ndvi_flag==3)

    aryBand[cloud_mask]=101
    aryBand[water_mask]=102
    aryBand[nan_mask]=103
    return aryBand
def create_ndvi_figure(out_data,out_tiff_file):
    # if not os.path.isfile(o_path):
    #     print 'there is no ndvi hdf file,%s',o_path
    #     return
    # out_path, out_name = os.path.split(o_path)
    # out_tiff_file = os.path.join(out_path, out_name.split('.')[0] + '.tiff')

    if os.path.isfile(out_tiff_file):
        print 'there is already exist output file :%s' % (out_tiff_file)
        return
    
    aryBand=get_ndvi_data(out_data)
    lut_map = np.loadtxt('lib/ndvi_symbol.txt', dtype=np.int)
    arrR,arrG,arrB=apply_lut_stretch(aryBand,lut_map)
    create_rgb(arrR, arrG, arrB, out_tiff_file)

# ######################## 程序全局入口 ##############################
if __name__ == "__main__":
    print 'ndvi symbolize with color lookup table'
