# coding: UTF-8
'''
Created on 2019-05
@author: yanguoqing
it will draw all himawari-8 channel image(tiff/jpg),it include：
1. true-color and geo-color image 
2. original and broadcast data
3. all channel image
4. water vapour and long ir enhancement image
5. ........
'''

import sys
import os
import h5py
import yaml
import numpy as np
from osgeo import gdal
from osgeo import osr
import datetime

# import threadpool
from lib.com_lib_plot_rayleigh import get_rayleigh_scatter

BandNames = {"B01": "NOMChannelVIS0046",
             "B02": "NOMChannelVIS0051",
             "B03": "NOMChannelVIS0064",
             "B04": "NOMChannelVIS0086",
             "B05": "NOMChannelVIS0160",
             "B06": "NOMChannelVIS0230",
             "B07": "NOMChannelIRX0390",
             "B08": "NOMChannelIRX0620",
             "B09": "NOMChannelIRX0700",
             "B10": "NOMChannelIRX0730",
             "B11": "NOMChannelIRX0860",
             "B12": "NOMChannelIRX0960",
             "B13": "NOMChannelIRX1040",
             "B14": "NOMChannelIRX1120",
             "B15": "NOMChannelIRX1230",
             "B16": "NOMChannelIRX1330",
             }

enhancement_lut = {
    'B08': {
        'range': [150, 205, 225, 245, 260, 280, 330],
        'color': [[0, 255, 255], [0, 128, 0], [255, 255, 255], [30, 30, 255], [247, 247, 0], [255, 38, 38], [255, 38, 38]],
        'clip': False
    },
    'B09': {
        'range': [150, 210, 240, 250, 260, 300, 330],
        'color': [[0, 255, 255], [0, 128, 0], [255, 255, 255], [0, 0, 200], [247, 247, 0], [255, 38, 38], [255, 38, 38]],
        'clip': False
    },
    'B10': {
        'range': [150, 205, 245, 255, 270, 300, 330],
        'color': [[0, 255, 255], [0, 128, 0], [255, 255, 255], [0, 0, 200], [247, 247, 0], [255, 38, 38], [255, 38, 38]],
        'clip': False
    },
    'B13': {
        'range': [173, 183, 193, 203, 213, 223, 233, 243],
        'color': [[255, 255, 255], [210, 210, 210], [0, 0, 0], [255, 26, 0], [230, 255, 0], [0, 198, 26], [0, 43, 138], [0, 255, 255]],
        'clip': True
    }
}


class ReadInYaml():

    def __init__(self, inFile):
        """
        读取yaml格式配置文件
        """
        if not os.path.isfile(inFile):
            print 'Not Found %s' % inFile
            sys.exit(-1)

        with open(inFile, 'r') as stream:
            cfg = yaml.load(stream)
        self.ymd_hm = cfg['INFO']['ymd']
        self.pair = cfg['INFO']['pair']
        self.dtype = cfg['INFO']['dtype']
        self.res = cfg['PROJ']['res']
        self.lt_lat = cfg['PROJ']['lt_lat']
        self.lt_lon = cfg['PROJ']['lt_lon']
        self.ipath = cfg['PATH']['ipath']
        self.ipath_geo = cfg['PATH']['ipath_geo']
        self.opath = cfg['PATH']['opath']
        self.log = cfg['PATH']['log']


class HMW8_HDF(object):

    def __init__(self):
        self.NAV_PATH = "/DATA2/KTS/CMA_KTS/COM/H08_GEO/GLOBAL/fygatNAV.Himawari08.xxxxxxx.2km001.hdf5"
       # self.NAV_PATH_R0500 = "/gpfs02/HMW8/himawari_src/pub_data/AHI8_OBI_NAV_R0500.hdf"

    def load_NAV(self, fpath):

        h5File = h5py.File(fpath, 'r')
        self.lons = h5File.get('pixel_longitude').value
        self.lats = h5File.get('pixel_latitude').value
        #self.dems = h5File.get('surface_elevation').value
        h5File.close()

    def load_HMW8(self, Band, fpath, res):
        dset_name = BandNames[Band]
        if res == 0.02:
            dset_name += "_2000"
        elif res == 0.01:
            dset_name += "_1000"
        elif res == 0.005:
            dset_name += "_0500"
        else:
            dset_name += "_4000"
        h5File = h5py.File(fpath, 'r')

        dset = h5File.get(dset_name)
        scale = dset.attrs['scale_factor']
        offset = dset.attrs['add_offset']
        dset_value = dset.value
        h5File.close()
        dset_value[dset_value > 50000] = 0
        dset_real = dset_value * scale + offset
        return dset_real

    def load_dems(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_surface_elevation").value
        h5File.close()
        return dset

    def load_space_mask(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_space_mask").value
        h5File.close()
        return dset

    def load_surface_type(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_surface_type").value
        h5File.close()
        return dset

    def load_snow_mask(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_snow_mask").value
        h5File.close()
        return dset

    def load_land_mask(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_land_mask").value
        h5File.close()
        return dset

    def load_ecosystem_type(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_ecosystem_type").value
        h5File.close()
        return dset

    def load_desert_mask(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_desert_mask").value
        h5File.close()
        return dset

    def load_coast_mask(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_coast_mask").value
        h5File.close()
        return dset

    def load_SunZenith(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("NOMSunZenith").value
        h5File.close()
        return dset * 0.01

    def load_SunAzimuth(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("NOMSunAzimuth").value
        h5File.close()
        return dset * 0.01

    def load_NOMAzimuth(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("NOMAzimuth").value
        h5File.close()
        return dset * 0.01

    def load_ViewZenith(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_satellite_zenith_angle").value
        h5File.close()
        return dset

    def load_ViewAzimuth(self, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get("pixel_satellite_azimuth_angle").value
        h5File.close()
        return dset


def rgb_to_hsv(rgb):
    # Translated from source of colorsys.rgb_to_hsv
    # r,g,b should be a numpy arrays with values between 0 and 255
    # rgb_to_hsv returns an array of floats between 0.0 and 1.0.
    rgb = rgb.astype('float')
    hsv = np.zeros_like(rgb)
    # in case an RGBA array was passed, just copy the A channel
    hsv[..., 3:] = rgb[..., 3:]
    r, g, b = rgb[..., 0], rgb[..., 1], rgb[..., 2]
    maxc = np.max(rgb[..., :3], axis=-1)
    minc = np.min(rgb[..., :3], axis=-1)
    hsv[..., 2] = maxc
    mask = maxc != minc
    hsv[mask, 1] = (maxc - minc)[mask] / maxc[mask]
    rc = np.zeros_like(r)
    gc = np.zeros_like(g)
    bc = np.zeros_like(b)
    rc[mask] = (maxc - r)[mask] / (maxc - minc)[mask]
    gc[mask] = (maxc - g)[mask] / (maxc - minc)[mask]
    bc[mask] = (maxc - b)[mask] / (maxc - minc)[mask]
    hsv[..., 0] = np.select(
        [r == maxc, g == maxc], [bc - gc, 2.0 + rc - bc], default=4.0 + gc - rc)
    hsv[..., 0] = (hsv[..., 0] / 6.0) % 1.0
    return hsv


def hsv_to_rgb(hsv):
    # Translated from source of colorsys.hsv_to_rgb
    # h,s should be a numpy arrays with values between 0.0 and 1.0
    # v should be a numpy array with values between 0.0 and 255.0
    # hsv_to_rgb returns an array of uints between 0 and 255.
    rgb = np.empty_like(hsv)
    rgb[..., 3:] = hsv[..., 3:]
    h, s, v = hsv[..., 0], hsv[..., 1], hsv[..., 2]
    i = (h * 6.0).astype('uint8')
    f = (h * 6.0) - i
    p = v * (1.0 - s)
    q = v * (1.0 - s * f)
    t = v * (1.0 - s * (1.0 - f))
    i = i % 6
    conditions = [s == 0.0, i == 1, i == 2, i == 3, i == 4, i == 5]
    rgb[..., 0] = np.select(conditions, [v, q, p, p, t, v], default=v)
    rgb[..., 1] = np.select(conditions, [v, v, v, q, p, p], default=t)
    rgb[..., 2] = np.select(conditions, [v, p, t, v, v, q], default=p)
    return rgb.astype('uint8')


def norm255(argArry):
    '''
    把原通道数据集转换成RGB数据集
    argArry：通道数据集
    return：RGB数据集
    '''
    mask = (argArry <= 0)
    retArry = np.ma.masked_where(mask, argArry)
    maxValue = np.ma.max(retArry)
    minValue = np.ma.min(retArry)
#     print(maxValue, minValue)
    if maxValue == minValue:
        return argArry
    retArry = (retArry - minValue) * 255. / \
        (maxValue - minValue)  # 数值拉伸到 0 ~ 255
    retArry = np.ma.filled(retArry, 0)
    return retArry.astype('uint8')


def cira_stretch(argAry):
    # cira stretch method
    argAry[argAry <= 0] = 0.001
    mask = (argAry <= 0)
    ary = np.ma.masked_where(mask, argAry)
    log_root = np.log10(0.0223)
    denom = (1 - log_root) * 0.75
    ary = np.log10(ary)
    ary = ary - log_root
    ary = ary / denom
    return ary


def linearStretch(argArry, percent=0.02, where="both"):
    '''
    线性拉伸, 去除最大最小的percent的数据，然后拉伸
    argArry：通道数据集
    percent：最大最小部分不参与拉伸的百分比
    return：RGB数据集
    '''
#     cv2.equalizeHist(argArry, argArry)
#     return argArry

    hist, bins = np.histogram(argArry, 256)

    pixelnum = sum(hist)
    cdf = hist.cumsum()  # 计算累积直方图
    cdf_rev = hist[::-1].cumsum()

    i1 = 0
    i2 = 255
    if 1 > percent > 0:
        if where == "low":
            for i in xrange(len(cdf)):
                if cdf[i] > pixelnum * percent and i1 == 0:
                    i1 = i
        if where == "high":
            for i in xrange(len(cdf_rev)):
                if cdf_rev[i] > pixelnum * percent and i2 == 255:
                    i2 = len(cdf_rev) - i
        if where == "both":
            for i in xrange(len(cdf)):
                if cdf[i] > pixelnum * percent and i1 == 0:
                    i1 = i
                if cdf_rev[i] > pixelnum * percent and i2 == 255:
                    i2 = len(cdf_rev) - i

    index1 = np.where(argArry < i1)
    index2 = np.where(argArry >= i2)
    mask = np.logical_or(argArry < i1, argArry >= i2)
    retArry = np.ma.masked_where(mask, argArry)
    maxValue = np.max(retArry)
    minValue = np.min(retArry)

    if maxValue == minValue:
        return argArry

    retArry = (retArry - minValue) * 255. / \
        (maxValue - minValue)  # 数值拉伸到 0 ~ 255
    retArry = np.ma.filled(retArry, 0).astype('uint8')
    retArry[index1] = 0
    retArry[index2] = 255

    return retArry


def customStretch(argArry, s_type='n'):
    '''   
    Table 3: Non-­‐‑linear Brightness Enhancement Table (non cloud)
    Input Brightness Output Brightness
    0 0
    30 110
    60 160
    120 210
    190 240
    255 255
    '''
#     fromList = [0, 30, 60, 120, 190, 255]
#     toList = [0, 110, 160, 210, 240, 255]

    # fromList = [0, 30, 60, 120, 190, 255]
    # toList = [0, 110, 160, 210, 240, 255]
    s_list = {
        'n': {'fromlist': [0, 30, 60, 120, 190, 255], 'tolist': [0, 110, 160, 210, 240, 255]},
        'r': {'fromlist': [0, 33, 100, 255], 'tolist': [1, 14, 124, 255]},
        'g': {'fromlist': [0, 38, 107, 255], 'tolist': [0, 13, 130, 255]},
        'b': {'fromlist': [0, 47, 116, 255], 'tolist': [0, 12, 138, 255]}
    }
    fromList = s_list[s_type]['fromlist']
    toList = s_list[s_type]['tolist']

    retArry = np.zeros_like(argArry, dtype='float')

    for i in range(len(fromList) - 1):
        from_min = fromList[i]
        from_max = fromList[i + 1]
        to_min = toList[i]
        to_max = toList[i + 1]
        index = np.where(
            np.logical_and(argArry > from_min, argArry <= from_max))
        retArry[index] = (argArry[index] - from_min) * float(to_max -
                                                             to_min) / float(from_max - from_min) + to_min  # 数值拉伸到

    return retArry.astype('uint8')


def stretch_rgb(aryArry, linear, linear_type):
    # apply cira and linear stretch method to enhance rgb effect to more clear
    aryBand = cira_stretch(aryArry)
    arrBand = norm255(aryBand)
    if linear > 0:
        arrBand = linearStretch(arrBand, linear / 100., linear_type)

    return arrBand


def color_dict(gradient):
    ''' Takes in a list of RGB sub-lists and returns dictionary of
    colors in RGB form for use in a graphing function defined later on '''
    return {
        "r": [RGB[0] for RGB in gradient],
        "g": [RGB[1] for RGB in gradient],
        "b": [RGB[2] for RGB in gradient]}


def linear_gradient(start_rgb, finish_rgb, n=10):
    ''' returns a gradient list of (n) colors between
    two rgb colors. start_rgb and finish_rgb '''
    # Starting and ending colors in RGB form
    # Initilize a list of the output colors with the starting color
    RGB_list = [start_rgb]
    # Calcuate a color at each evenly spaced value of t from 1 to n
    for t in range(1, n):
        # Interpolate RGB vector for color at the current value of t
        curr_vector = [
            int(start_rgb[j] + (float(t) / (n - 1)) * (finish_rgb[j] - start_rgb[j])) for j in range(3)
        ]
        # Add it to our list of output colors
        RGB_list.append(curr_vector)
    return RGB_list


def create_rgb(aryR, aryG, aryB, out_tiff_path, res, LonW, LatN, stretch_flag, linear=2.):

    if stretch_flag:

        arrR = stretch_rgb(aryR, linear, 'low')
        arrG = stretch_rgb(aryG, linear, 'low')
        arrB = stretch_rgb(aryB, linear, 'low')

    else:
        arrR = aryR
        arrG = aryG
        arrB = aryB

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
    dst_ds.GetRasterBand(2).WriteArray(arrG)  # write g-band to the raster
    dst_ds.GetRasterBand(3).WriteArray(arrB)  # write b-band to the raster
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


def create_gray_tiff(aryBand, out_tiff_file, res, LonW, LatN, linear=2.):
    arrBand = norm255(aryBand)
    if linear > 0:
        arrBand = linearStretch(arrBand, linear / 100., "low")
    ny, nx = arrBand.shape
    xres = res
    yres = res
    geotransform = (LonW, xres, 0, LatN, 0, -yres)
    dst_ds = gdal.GetDriverByName('GTiff').Create(
        out_tiff_file, nx, ny, 1, gdal.GDT_Byte, ["COMPRESS=LZW"])
    dst_ds.SetGeoTransform(geotransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    dst_ds.SetProjection(srs.ExportToWkt())
    dst_ds.GetRasterBand(1).WriteArray(arrBand)
    dst_ds.FlushCache()  # write to disk
    dst_ds = None

    out_path, file_name = os.path.split(out_tiff_file)
    out_jpg_path = os.path.join(out_path, file_name.split('.')[0] + '.jpg')
    dst_ds = gdal.GetDriverByName('MEM').Create(
        out_jpg_path, nx, ny, 3, gdal.GDT_Byte)
    dst_ds.GetRasterBand(1).WriteArray(arrBand)
    dst_ds.GetRasterBand(2).WriteArray(arrBand)
    dst_ds.GetRasterBand(3).WriteArray(arrBand)
    png_dirver = gdal.GetDriverByName("JPEG")
    png_ds = png_dirver.CreateCopy(out_jpg_path, dst_ds)
    png_ds.FlushCache()  # write to disk
    dst_ds = None
    png_ds = None


def create_enhance_tiff(arrR, arrG, arrB, out_tiff_path, res, LonW, LatN):
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
    dst_ds.GetRasterBand(2).WriteArray(arrG)  # write g-band to the raster
    dst_ds.GetRasterBand(3).WriteArray(arrB)  # write b-band to the raster
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


def enhancement_band(aryBand, lut_map, step=10):
    # enhancement watervapour channel(6.2,7.0,7.3) and clean long ir
    # window(10.4)
    t_1 = datetime.datetime.now()
    print 'enhance band starting... at ', t_1

    value_range = lut_map['range']
    colors = lut_map['color']
    color_maps = linear_gradient(
        colors[0], colors[1], (value_range[1] - value_range[0]) * step)
    for col in range(1, len(colors) - 1):
        color_map = linear_gradient(
            colors[col], colors[col + 1], (value_range[col + 1] - value_range[col]) * step)
        for cm in color_map:
            color_maps.append(cm)

    cms = np.array(color_maps)
    if lut_map['clip']:
        aryBg = norm255(aryBand)
        aryBg = linearStretch(aryBg,  2.0 / 100., "low")
        aryBg = 255-aryBg
    range_min = value_range[0]
    range_max = value_range[-1]

    min_idx = np.where(aryBand < range_min)
    max_idx = np.where(aryBand > range_max)
    aryBand[min_idx] = range_min
    aryBand[max_idx] = range_max

    aryR = np.zeros_like(aryBand)
    aryG = np.zeros_like(aryBand)
    aryB = np.zeros_like(aryBand)
    aryR = aryR.astype(np.int8)
    aryG = aryG.astype(np.int8)
    aryB = aryB.astype(np.int8)
    cm_idx = np.round(
        (aryBand - range_min) / (range_max - range_min) * (cms.shape[0] - 1))

    ny, nx = aryBand.shape

    # loop cols of cm_idx get r,g,b
    col_range = int(nx / cms.shape[0]) + 1
    col_step = min(cms.shape[0], nx)
    for r in range(ny):
        for c in range(col_range):
            slice_s = c * col_step
            slice_e = min((c + 1) * col_step, nx)
            slice_idx = cm_idx[r, slice_s:slice_e].astype(np.int16)

            aryR[r, slice_s:slice_e] = cms[:, 0][slice_idx]
            aryG[r, slice_s:slice_e] = cms[:, 1][slice_idx]
            aryB[r, slice_s:slice_e] = cms[:, 2][slice_idx]

    if lut_map['clip']:
        aryR[max_idx] = aryBg[max_idx]
        aryG[max_idx] = aryBg[max_idx]
        aryB[max_idx] = aryBg[max_idx]

    t_2 = datetime.datetime.now()
    print 'enhance band ending... at ', t_2
    print 'enhance band using.. ', t_2 - t_1
    return aryR, aryG, aryB


def enlarge_2d(aryBand, r_ratio):
    row, col = aryBand.shape
    ret = np.full((row * col, r_ratio * r_ratio), -999.0, dtype="f")
    i, j = np.mgrid[0:row * col:1, 0:r_ratio * r_ratio:1]
    ret[i, j] = aryBand.reshape(-1,)[i]
    ret = np.swapaxes(ret.reshape(row, col, r_ratio, r_ratio), 1, 2)
    resized_band = ret.reshape(row * r_ratio, col * r_ratio)
    return resized_band


def get_rgb_data(i_file):

    bg_data = gdal.Open(i_file)
    col = bg_data.RasterXSize
    row = bg_data.RasterYSize
    bg_red = bg_data.GetRasterBand(1).ReadAsArray(0, 0, col, row)
    bg_green = bg_data.GetRasterBand(2).ReadAsArray(0, 0, col, row)
    bg_blue = bg_data.GetRasterBand(3).ReadAsArray(0, 0, col, row)

    return bg_red, bg_green, bg_blue


def get_gray_data(i_file):
    bg_data = gdal.Open(i_file)
    col = bg_data.RasterXSize
    row = bg_data.RasterYSize
    bg_band = bg_data.GetRasterBand(1).ReadAsArray(0, 0, col, row)
    return bg_band


def create_true_color(i_file, g_file, res):
    t_1 = datetime.datetime.now()
    print 'true color starting at ', t_1

    h8 = HMW8_HDF()
    b1 = h8.load_HMW8('B03', i_file, res)
    b2 = h8.load_HMW8('B02', i_file, res)
    b3 = h8.load_HMW8('B01', i_file, res)
    b4 = h8.load_HMW8('B04', i_file, res)
    # calculate the rayleigh scatter contributor --yan
    sz = h8.load_SunZenith(i_file)
    sat_zenith = h8.load_ViewZenith(g_file)
    sa = h8.load_SunAzimuth(i_file)
    sat_azimuth = h8.load_ViewAzimuth(g_file)
    diff_azimuth = np.abs(sa - sat_azimuth)
    diff_azimuth[np.where(diff_azimuth > 180)] = 360 - \
        diff_azimuth[np.where(diff_azimuth > 180)]

    # import multi-thread to process multi task at the same time
    # conf_list=[(None,{'bandArr':b1,'bandname':'B03','sun_zenith':sz,'sat_zenith':sat_zenith,'azimuth_diff':diff_azimuth
    # }),(None,{'bandArr':b2,'bandname':'B02','sun_zenith':sz,'sat_zenith':sat_zenith,'azimuth_diff':diff_azimuth
    # }),(None,{'bandArr':b3,'bandname':'B01','sun_zenith':sz,'sat_zenith':sat_zenith,'azimuth_diff':diff_azimuth
    # })]
    # pool = threadpool.ThreadPool(3)

    # t1=datetime.datetime.now()
    # if len(conf_list) > 0:
    #     requests = threadpool.makeRequests(get_rayleigh_scatter, conf_list, None)
    #     [pool.putRequest(req) for req in requests]
    #     pool.wait()

    # t2=datetime.datetime.now()
    # print 'multi thread using ..',t2-t1

    # location where will be filled with zero
    fill_zero_idx = np.where(sz > 88)

    t3 = datetime.datetime.now()
    b1 = get_rayleigh_scatter(b1, 'B03', sz, sat_zenith, diff_azimuth)
    b2 = get_rayleigh_scatter(b2, 'B02', sz, sat_zenith, diff_azimuth)
    b3 = get_rayleigh_scatter(b3, 'B01', sz, sat_zenith, diff_azimuth)

    t4 = datetime.datetime.now()
    print 'regular using ..', t4 - t3
    # if (not b1 or not b2 or not b3):
    # if (type(b1)==bool or type(b2)==bool or type(b3)==bool):
    #     print "NO 2km angle file, at %s %s" % (ymd, hm)
    #     return False
    # remove mosaic glass effect cause by negative number--yan
    # any channel value equal 0 then set all the channel value 100(1-500,no
    # apprent diffirenece)

    # conditions1 = np.logical_or.reduce((b1 <= 0, b2 <= 0, b3 <= 0))
    # conditions2 = np.logical_and.reduce((b1 == 0, b2 == 0, b3 == 0))
    # conditions = np.logical_xor(conditions1, conditions2)
    # indexs = np.where(conditions)
    # b1[indexs] = 0.01
    # b2[indexs] = 0.01
    # b3[indexs] = 0.01
    # band2 90% + band4 (植被通道) 10%, 为了让绿色更有边界感

    b2 = b2 * 0.95 + b4 * 0.05

    b1[fill_zero_idx] = 0
    b2[fill_zero_idx] = 0
    b3[fill_zero_idx] = 0

    # 除cos太阳天顶角

    sz[sz > 70] = 70
    sz[sz < -70] = -70
    cossz = np.cos(np.deg2rad(sz))
    b1 = b1 / cossz
    b2 = b2 / cossz
    b3 = b3 / cossz

    t_2 = datetime.datetime.now()
    print 'true color ending... at ', t_2
    print 'true color using.. ', t_2 - t_1

    return b1, b2, b3


def create_geo_color(t_red, t_green, t_blue, lr_i_file, i_file, h_res, l_res):
    t_1 = datetime.datetime.now()
    print 'geo color starting... at ', t_1

    t_3 = datetime.datetime.now()
    print 'preparing night ir data starting... at ', t_3
    h8 = HMW8_HDF()
    l_ir_2 = h8.load_HMW8('B13', lr_i_file, l_res)
    l_ir_1 = h8.load_HMW8('B07', lr_i_file, l_res)

    soz = h8.load_SunZenith(i_file)
    resize_ratio = int(l_res / h_res)
    ir_2 = enlarge_2d(l_ir_2, resize_ratio)
    ir_1 = enlarge_2d(l_ir_1, resize_ratio)
    base_row, base_col = t_red.shape
    ir_2 = ir_2[:base_row, :base_col]
    ir_1 = ir_1[:base_row, :base_col]
    diff_ir = ir_2 - ir_1
    diff_ir[np.where(diff_ir > 6)] = 6
    diff_ir[np.where(diff_ir < 0)] = 0
    land_mask_file = 'lib/land_mask_hsd.tif'
#     if l_res == 0.02:
#         land_mask_file = 'lib/land_mask_hsd.tif'
#     else:
#         land_mask_file = 'lib/land_mask_cast.tif'

    land_mask = gdal.Open(land_mask_file)
    col = land_mask.RasterXSize
    row = land_mask.RasterYSize
    lm = land_mask.GetRasterBand(1).ReadAsArray(0, 0, col, row)
    land_sea_mask = np.ones(lm.shape, dtype='uint8')
    land_sea_mask[np.where(lm > 100)] = 2

    fog_mask = np.logical_or(np.logical_and(
        land_sea_mask == 2, diff_ir > 1), np.logical_and(land_sea_mask == 1, diff_ir > 0))
    # fog_mask=diff_ir>0

    night_fog = norm255(diff_ir)

    norm_ir_2 = norm255(ir_2)
    inver_ir_2 = 255. - norm_ir_2
    fog_mask = np.logical_and(fog_mask, inver_ir_2 < 128)
    inver_ir_2[fog_mask] = 0

    night_band = norm255(inver_ir_2)

    bg_light_alpha = (inver_ir_2 - 0) / 255 * 1.0
    bg_light_beta = 1 - bg_light_alpha
    light_file = 'lib/night_light_hsd.tif'
#     if l_res == 0.02:
#         light_file = 'lib/night_light_hsd.tif'
#     else:
#         light_file = 'lib/night_light_cast.tif'

    bg_red, bg_green, bg_blue = get_rgb_data(light_file)
    night_green = night_band * bg_light_alpha + bg_green * bg_light_beta
    night_blue = night_band * bg_light_alpha + bg_blue * bg_light_beta
    night_red = night_band * bg_light_alpha + bg_red * bg_light_beta

    night_blue[fog_mask] = night_fog[fog_mask]
    night_green[fog_mask] = night_fog[fog_mask]*0.6
    night_red[fog_mask] = 0

    night_blue = linearStretch(night_blue, 0.02, 'low')
    night_green = linearStretch(night_green, 0.02, 'low')
    night_red = linearStretch(night_red, 0.02, 'low')
    # night_red[fog_mask]=night_fog[fog_mask]

    t_4 = datetime.datetime.now()
    print 'preparing night ir data ending... at ', t_4
    print 'prepareing night ir data using .. ', t_4 - t_3

    soz_min = 75
    soz_max = 88

    cos_soz_min = np.cos(np.deg2rad(soz_min))
    cos_soz_max = np.cos(np.deg2rad(soz_max))

    soz[np.where(soz < soz_min)] = soz_min
    soz[np.where(soz > soz_max)] = soz_max

    cos_soz = np.cos(np.deg2rad(soz))
    day_night_weight = (cos_soz - cos_soz_max) / \
        (cos_soz_min - cos_soz_max) * 1.0
    night_beta = 1 - day_night_weight
# print t_red.shape, day_night_weight.shape, night_red.shape,
# night_beta.shape
    geo_color_red = t_red * day_night_weight + night_red * night_beta
    geo_color_green = t_green * day_night_weight + night_green * night_beta
    geo_color_blue = t_blue * day_night_weight + night_blue * night_beta

    t_2 = datetime.datetime.now()
    print 'geo color ending... at ', t_2
    print 'geo color using.. ', t_2 - t_1
    return geo_color_red, geo_color_green, geo_color_blue


def create_rgb_figure(i_file, lr_i_file, o_path, g_file, lr_g_file, true_color_flag, geo_color_flag, dtype, res, l_res, LonW, LatN, pair, ymd, hm):
    h8 = HMW8_HDF()
    soz = h8.load_SunZenith(i_file)
    max_soz = np.max(soz)
    min_soz = np.min(soz)
    if min_soz > 80:
        full_night_flag = True
    else:
        full_night_flag = False

    if max_soz < 80:
        full_day_flag = True
    else:
        full_day_flag = False

    if hm > "1000" and hm < "2200":
        night_flag = True
    else:
        night_flag = False

    if true_color_flag:
        out_name = pair + "_" + str(ymd) + "_" + hm + "_" + "True_Color"
        out_tiff_file = os.path.join(o_path, out_name + ".tiff")

        if os.path.isfile(out_tiff_file):
            print 'there is already exist output file :%s' % (out_tiff_file)
            if geo_color_flag:
                out_name = pair + "_" + str(ymd) + "_" + hm + "_" + "Geo_Color"
                out_gc_tiff_file = os.path.join(o_path, out_name + ".tiff")
                if os.path.isfile(out_gc_tiff_file):
                    print 'there is already exist output file :%s' % (
                        out_gc_tiff_file)
                else:
                    t_red, t_green, t_blue = get_rgb_data(out_tiff_file)
                    if full_day_flag:
                        create_rgb(
                            t_red, t_green, t_blue, out_gc_tiff_file, res, LonW, LatN, False)
                    else:
                        aryR, aryG, aryB = create_geo_color(
                            t_red, t_green, t_blue, lr_i_file, i_file, res, l_res)
                        create_rgb(
                            aryR, aryG, aryB, out_gc_tiff_file, res, LonW, LatN, False)
        else:
            if not full_night_flag:
                b1, b2, b3 = create_true_color(i_file, g_file, res)
                if not night_flag:
                    create_rgb(b1, b2, b3, out_tiff_file,
                               res, LonW, LatN, True)

            if geo_color_flag:
                out_name = pair + "_" + str(ymd) + "_" + hm + "_" + "Geo_Color"
                out_gc_tiff_file = os.path.join(o_path, out_name + ".tiff")
                if os.path.isfile(out_gc_tiff_file):
                    print 'there is already exist output file :%s' % (
                        out_gc_tiff_file)
                else:
                    if night_flag:
                        if not full_night_flag:
                            t_red = stretch_rgb(b1, 1.0, 'low')
                            t_green = stretch_rgb(b2, 1.0, 'low')
                            t_blue = stretch_rgb(b3, 1.0, 'low')
                        else:
                            t_red = np.zeros(soz.shape, dtype='int8')
                            t_green = np.zeros(soz.shape, dtype='int8')
                            t_blue = np.zeros(soz.shape, dtype='int8')
                    else:
                        t_red, t_green, t_blue = get_rgb_data(out_tiff_file)
                    if full_day_flag:
                        create_rgb(
                            t_red, t_green, t_blue, out_gc_tiff_file, res, LonW, LatN, False)
                    else:
                        aryR, aryG, aryB = create_geo_color(
                            t_red, t_green, t_blue, lr_i_file, i_file, res, l_res)
                        create_rgb(
                            aryR, aryG, aryB, out_gc_tiff_file, res, LonW, LatN, False)

    else:
        if geo_color_flag:
            out_name = pair + "_" + str(ymd) + "_" + hm + "_" + "Geo_Color"
            out_gc_tiff_file = os.path.join(o_path, out_name + ".tiff")
            if os.path.isfile(out_gc_tiff_file):
                print 'there is already exist output file :%s' % (
                    out_gc_tiff_file)

            else:
                if not full_night_flag:

                    h8 = HMW8_HDF()
                    base_band = h8.load_HMW8('B03', i_file, res)
                    out_name = pair + "_" + str(ymd) + "_" + hm + "_" + "B03"
                    out_tiff_file = os.path.join(o_path, out_name + ".tiff")
                    if os.path.isfile(out_tiff_file):
                        # print 'there is already exist output file :%s' %
                        # (out_tiff_file)
                        arrBand = get_gray_data(out_tiff_file)
                    else:
                        arrBand = norm255(base_band)
                        arrBand = linearStretch(arrBand, 0.02, "low")
                else:
                    arrBand = np.zeros(soz.shape, dtype='int8')

                if full_day_flag:
                    #create_gray_tiff(
                    #    arrBand, out_gc_tiff_file, res, LonW, LatN)
                    create_rgb(
                        arrBand, arrBand, arrBand, out_gc_tiff_file, res, LonW, LatN, False)
                else:
                    aryR, aryG, aryB = create_geo_color(
                        arrBand, arrBand, arrBand, lr_i_file, i_file, res, l_res)
                    create_rgb(
                        aryR, aryG, aryB, out_gc_tiff_file, res, LonW, LatN, False)


def create_channel_figure(i_file, o_path, g_file, bandList, res, LonW, LatN, pair, ymd, hm):
    h8 = HMW8_HDF()
    for band in bandList:

        out_name = pair + "_" + str(ymd) + "_" + hm + "_" + band
        out_tiff_file = os.path.join(o_path, out_name + ".tiff")
        if os.path.isfile(out_tiff_file):
            print 'there is already exist output file :%s' % (out_tiff_file)
            continue
        aryBand = h8.load_HMW8(band, i_file, res)
        # out_pic_file=os.path.join(o_path,out_name+".jpg")
        # if os.path.isfile(out_tiff_file):
        #     print 'there is already exist output file :%s'%(out_tiff_file)
        #     return
        if band in ['B08', 'B09', 'B10', 'B13']:
            lut = enhancement_lut[band]
            aryR, aryG, aryB = enhancement_band(aryBand, lut)
            create_enhance_tiff(
                aryR, aryG, aryB, out_tiff_file, res, LonW, LatN)
        else:
            create_gray_tiff(aryBand, out_tiff_file, res, LonW, LatN)

    return True


def run_job(interface_file):
    print interface_file

    # read yaml interface_file to get basic params
    in_cfg = ReadInYaml(interface_file[0])
    # in_log = LogServer(in_cfg.log)
    # in_log.info(u'[%s] [%s] 葵花L1合成开始' % (in_cfg.pair, in_cfg.ymd))
    # 创建输出
    out_path = in_cfg.opath
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    input_path = in_cfg.ipath
    if not os.path.isfile(input_path):
        print 'there is no this file:%s' % (input_path)
        return

    dtype = in_cfg.dtype
    res = in_cfg.res
    lt_lat = in_cfg.lt_lat
    lt_lon = in_cfg.lt_lon
    nav_path = in_cfg.ipath_geo
    ymd_hm = in_cfg.ymd_hm
    ymd = str(ymd_hm).split("_")[0]
    hm = str(ymd_hm).split("_")[1]
    pair = in_cfg.pair

    # set lt_lat,lt_lon as center pixel location
    lt_lat = lt_lat + res / 2.0
    lt_lon = lt_lon - res / 2.0

    # out_name=pair+"_True_Color_"+ymd+"_"+hm
    # out_tiff=os.path.join(out_path,out_name,'.tiff')
    # out_pic=os.path.join(out_path,out_name,'.jpg')
    # if os.path.isfile(out_tiff):
    #     print 'There is already exist output file:%s'%(out_tiff)
    #     return
    # if os.path.isfile(out_pic):
    #     print 'There is already exist output file:%s'%(out_pic)
    #     return

    # utc 2200-1000 create true_color_product

    if dtype == 'ORIGINAL':
        # original data format
        l_res = 0.02
        if res == 0.01:
            true_color_flag = True
            lr_input_path_dir = os.path.dirname(input_path)
            lr_input_path_name = os.path.basename(
                input_path).replace('1000M', '2000M')
            lr_input_path = os.path.join(lr_input_path_dir, lr_input_path_name)

            lr_nav_path_dir = os.path.dirname(nav_path)
            lr_nav_path_name = os.path.basename(
                nav_path).replace('1000M', '2000M')
            lr_nav_path = os.path.join(lr_nav_path_dir, lr_nav_path_name)

            if not os.path.isfile(lr_input_path):
                print 'there is no this file:%s' % (lr_input_path)
                geo_color_flag = False
            else:
                geo_color_flag = True
                true_color_flag = True

        else:
            true_color_flag = False
            geo_color_flag = False

        if res == 0.01:
            bandList = ["B01", "B02", "B04"]
        elif res == 0.02:
            bandList = ["B05", "B06", "B07", "B08", "B09",
                        "B10", "B11", "B12", "B13", "B14", "B15", "B16"]
        else:
            bandList = ["B03"]

        # if hm > "1000" and hm < "2200":
        #     if res < 0.02:
        #         print 'night time,there is no available vis channle band'
        #         return
    else:
        # broadcast data format

        true_color_flag = False
        l_res = 0.04
        if res == 0.01:
            lr_input_path_dir = os.path.dirname(input_path)
            lr_input_path_name = os.path.basename(
                input_path).replace('1000M', '4000M')
            lr_input_path = os.path.join(lr_input_path_dir, lr_input_path_name)

            lr_nav_path_dir = os.path.dirname(nav_path)
            lr_nav_path_name = os.path.basename(
                nav_path).replace('1000M', '4000M')
            lr_nav_path = os.path.join(lr_nav_path_dir, lr_nav_path_name)

            if not os.path.isfile(lr_input_path):
                print 'there is no this file:%s' % (lr_input_path)
                geo_color_flag = False
            else:
                geo_color_flag = True
                # true_color_flag=True
        else:
            geo_color_flag = False

        if res == 0.01:
            bandList = ['B03']
        elif res == 0.04:
            bandList = ["B04", "B05", "B06", "B07", "B08", "B09",
                        "B10", "B11", "B12", "B13", "B14", "B15", "B16"]
        else:
            # broadcast 3.9um channel 2km resolution at 0800-2150(utc) every
            # day if necessary
            bandList = ['B07']
    # if hm > "1000" and hm < "2200":
    #     true_color_flag = False

    if true_color_flag or geo_color_flag:
        create_rgb_figure(input_path, lr_input_path, out_path, nav_path, lr_nav_path,
                          true_color_flag, geo_color_flag, dtype, res, l_res, lt_lon, lt_lat, pair, ymd, hm)

    if hm > "1000" and hm < "2200":
        if res < 0.02:
            print 'night time,there is no available vis channle band'
            return
    create_channel_figure(
        input_path, out_path, nav_path, bandList, res, lt_lon, lt_lat, pair, ymd, hm)


if __name__ == '__main__':
    t_start = datetime.datetime.now()
    print 't1: ', t_start
    args = sys.argv[1:]
    run_job(args)
    t_end = datetime.datetime.now()
    print 't2: ', t_end
    print 'using ..', t_end - t_start
    # main()
