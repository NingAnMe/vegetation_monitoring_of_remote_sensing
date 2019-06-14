# coding: UTF-8
'''
Created on 2019-05-19
write by kts

it will use to convert himawari-8 l2 product to tiff/jpg file for webgis display
product-type:
Aerosol--Haze,PM2.5
Aviation--Fog(Low,Middle,High)
Surface--LST
Weather--QPE
'''

import sys
import os
import h5py
import yaml
import numpy as np
from osgeo import gdal
from osgeo import osr
from datetime import datetime, timedelta

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


product_alias_name = {
    'Aerosol': {'AOD': 'land_aerosol_optical_depth_550', 'PM': 'land_aer_mass_concentration'},
    'Aviation': {'FOG_MASK': 'fog_mask', 'FOG_L': 'fog_prob_0m_150m', 'FOG_M': 'fog_prob_150m_300m', 'FOG_H': 'fog_prob_300m_1500m'},
    'Weather': {'QPE': 'qpe'},
    'Surface': {'LST': 'land_surface_temperature'}
}

lst_clut = {
    'fromList': [-50, -30, -20, -10, -5, 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70],
    'toList': [(1, 1, 1), (36, 0, 216), (24, 28, 247), (40, 87, 255), (61, 135, 255), (86, 176, 255), (117, 211, 255), (153, 234, 255), (188, 249, 255), (234, 255, 255), (255, 255, 234), (255, 241, 188), (255, 214, 153), (255, 172, 117), (255, 120, 86), (255, 61, 61), (247, 39, 53), (216, 21, 47), (165, 0, 33), (165, 0, 33)]
}
aod_clut = {
    'fromList': [0, 0.01, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0],
    'toList': [(0, 0, 0), (243, 166, 255), (220, 1, 255), (149, 1, 255), (78, 1, 255), (7, 1, 255), (1, 64, 255), (1, 135, 255), (1, 205, 255), (1, 255, 234), (1, 255, 163), (1, 255, 92), (1, 255, 21), (50, 255, 1), (120, 255, 1), (191, 255, 1), (50, 255, 1), (255, 248, 1), (255, 177, 1), (255, 106, 1), (255, 35, 1), (178, 24, 1)]
}

pm_clut = {
    'fromList': [0, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 150, 180, 200, 250, 300],
    'toList': [(1, 97, 1), (43, 117, 1), (73, 138, 1), (103, 158, 1), (139, 181, 1), (176, 207, 1), (214, 230, 1), (255, 255, 1), (255, 229, 1), (255, 200, 1), (255, 166, 1), (255, 140, 1), (255, 111, 1), (255, 77, 1), (255, 255, 1), (255, 38, 1)]
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
        self.type = cfg["INFO"]['type']
        self.pair = cfg['INFO']['pair']
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
        else:
            dset_name += "_0500"
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get(dset_name).value
        h5File.close()

        dset[dset > 50000] = 0
        return dset

    def load_product(self, attr, fpath):
        h5File = h5py.File(fpath, 'r')
        dset = h5File.get(attr).value
        h5File.close()
        return dset

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
    # mask = (argArry <= 0)
    # retArry = np.ma.masked_where(mask, argArry)
    retArry = argArry
    maxValue = np.ma.max(retArry)
    minValue = np.ma.min(retArry)
#     print(maxValue, minValue)
    if maxValue == minValue:
        return argArry
    retArry = (retArry - minValue) * 255. / \
        (maxValue - minValue)  # 数值拉伸到 0 ~ 255
    retArry = np.ma.filled(retArry, 0)
    return retArry.astype('uint8')


def create_rgb(arrR, arrG, arrB, out_tiff_path, res, LonW, LatN):
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
    dst_ds.GetRasterBand(1).SetNoDataValue(1)  # set nodata value
    dst_ds.GetRasterBand(2).WriteArray(arrG)  # write g-band to the raster
    dst_ds.GetRasterBand(2).SetNoDataValue(1)  # set nodata value
    dst_ds.GetRasterBand(3).WriteArray(arrB)  # write b-band to the raster
    dst_ds.GetRasterBand(3).SetNoDataValue(1)  # set nodata valuer
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


def fill_zero_data(aryData):
    max_row, max_col = aryData.shape
    mask = aryData == 0
    condition = np.empty_like(mask)
    up = np.zeros_like(aryData)
    down = np.zeros_like(aryData)
    left = np.zeros_like(aryData)
    right = np.zeros_like(aryData)
    sum_region = np.zeros_like(aryData)
    count_region = np.zeros_like(aryData)
    condition = mask
    condition[0, :] = False
    condition[-1, :] = False
    condition[:, 0] = False
    condition[:, -1] = False
    index = np.where(condition)
    up[index[0], index[1]] = aryData[index[0] - 1, index[1]]
    down[index[0], index[1]] = aryData[index[0] + 1, index[1]]
    left[index[0], index[1]] = aryData[index[0], index[1] - 1]
    right[index[0], index[1]] = aryData[index[0], index[1] + 1]
    sum_region = up + down + left + right
    up[np.where(up > 0)] = 1
    down[np.where(down > 0)] = 1
    left[np.where(left > 0)] = 1
    right[np.where(right > 0)] = 1
    count_region = up + down + left + right

    fill_index = np.where(count_region > 1)
    aryData[fill_index] = sum_region[fill_index] / count_region[fill_index]

    return aryData


def apply_lut_class(array_values, out_tiff_path, clut, res, LonW, LatN):

    # classification symbolization use color lookup table
    arrR = np.zeros_like(array_values)
    arrG = np.zeros_like(array_values)
    arrB = np.zeros_like(array_values)
    fromList = clut['fromList']
    toList = clut['toList']
    for i in range(len(fromList) - 1):
        from_min = fromList[i]
        from_max = fromList[i + 1]
        to_rgb = toList[i + 1]
        index = np.where(
            np.logical_and(array_values >= from_min, array_values < from_max))
        arrR[index] = to_rgb[0]
        arrG[index] = to_rgb[1]
        arrB[index] = to_rgb[2]

    index = np.where(array_values >= fromList[-1])
    arrR[index] = toList[-1][0]
    arrG[index] = toList[-1][1]
    arrB[index] = toList[-1][2]

    index = np.where(array_values <= fromList[0])
    arrR[index] = toList[0][0]
    arrG[index] = toList[0][1]
    arrB[index] = toList[0][2]

    create_rgb(arrR, arrG, arrB, out_tiff_path, res, LonW, LatN)


def apply_lut_stretch(array_values, out_tiff_path, lut, res, LonW, LatN):
    # classification symbolization use color lookup table
    ny, nx = array_values.shape
    arrR = np.zeros_like(array_values)
    arrG = np.zeros_like(array_values)
    arrB = np.zeros_like(array_values)

    color_array = np.zeros((ny, nx, 3), np.uint8)

    for i in range(0, ny):
        for j in range(0, nx):
            color_array[i, j] = lut[np.int(array_values[i, j])]

    arrR = color_array[:, :, 0]
    arrG = color_array[:, :, 1]
    arrB = color_array[:, :, 2]

    create_rgb(arrR, arrG, arrB, out_tiff_path, res, LonW, LatN)


def create_figure(i_file, o_path, g_file, p_type, res, LonW, LatN, pair, ymd, hm):

    h8 = HMW8_HDF()

    if p_type == 'Surface':
        band_attr = product_alias_name[p_type]['LST']
        aryBand = h8.load_product(band_attr, i_file)
        out_name = pair + "_" + p_type + "_" + ymd + "_" + hm + "_" + 'LST'
        out_tiff_file = os.path.join(o_path, out_name + ".tiff")
        if os.path.isfile(out_tiff_file):
            print 'there is already exist output file :%s' % (out_tiff_file)
            return
        aryBand = aryBand - 273.15
        apply_lut_class(aryBand, out_tiff_file, lst_clut, res, LonW, LatN)
    elif p_type == 'Aerosol':
        band_attr = product_alias_name[p_type]['AOD']
        aryBand = h8.load_product(band_attr, i_file)
        out_name = pair + "_" + p_type + "_" + ymd + "_" + hm + "_" + 'AOD'
        out_tiff_file = os.path.join(o_path, out_name + ".tiff")
        if os.path.isfile(out_tiff_file):
            print 'there is already exist output file :%s' % (out_tiff_file)
            return
        aod_adjust_idx = np.where(np.logical_and(aryBand > 300, aryBand < 400))
        aryBand[aod_adjust_idx] = aryBand[aod_adjust_idx] * 2
        aod_adjust_idx = np.where(aryBand <= 300)
        aryBand[aod_adjust_idx] = aryBand[aod_adjust_idx] * 3
        aryBand = aryBand * 0.001
        apply_lut_class(aryBand, out_tiff_file, aod_clut, res, LonW, LatN)

    elif p_type == 'Weather':
        band_attr = product_alias_name[p_type]['QPE']
        aryBand = h8.load_product(band_attr, i_file)
        out_name = pair + "_" + p_type + "_" + ymd + "_" + hm + "_" + 'QPE'
        out_tiff_file = os.path.join(o_path, out_name + ".tiff")
        if os.path.isfile(out_tiff_file):
            print 'there is already exist output file :%s' % (out_tiff_file)
            return
        minRange = 0
        maxRange = 30
        aryBand = (aryBand - minRange) / maxRange * 255
        lut_map = np.loadtxt('lib/jet_color_map.txt', dtype=np.int)
        apply_lut_stretch(aryBand, out_tiff_file, lut_map, res, LonW, LatN)
    elif p_type == 'Aviation':
        fog_mask_attr = product_alias_name[p_type]['FOG_MASK']
        fog_mask = h8.load_product(fog_mask_attr, i_file)
        for fog_item in ['FOG_L', 'FOG_M', 'FOG_H']:
            band_attr = product_alias_name[p_type][fog_item]
            aryBand = h8.load_product(band_attr, i_file)
            aryBand[aryBand <= 2] = 0
            # aryBand=aryBand.astype(np.int8)
            out_name = pair + "_" + p_type + "_" + \
                ymd + "_" + hm + "_" + fog_item
            out_tiff_file = os.path.join(o_path, out_name + ".tiff")
            if os.path.isfile(out_tiff_file):
                print 'there is already exist output file :%s' % (out_tiff_file)
                return
            aryBand[np.where(fog_mask <> 1)] = 0
            minRange = 0
            maxRange = 100
            print "1111", np.max(aryBand), np.min(aryBand)
            aryBand = (aryBand - minRange) / maxRange * 255
            print "2222", np.max(aryBand), np.min(aryBand)
            aryBand = aryBand.astype(np.int8)
            lut_map = np.loadtxt('lib/jet_color_map.txt', dtype=np.int)
            apply_lut_stretch(aryBand, out_tiff_file, lut_map, res, LonW, LatN)
    else:
        print 'there is no this type product %s' % (p_type)
        return
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

    p_type = in_cfg.type
    res = in_cfg.res
    lt_lat = in_cfg.lt_lat
    lt_lon = in_cfg.lt_lon
    nav_path = in_cfg.ipath_geo
    ymd_hm = in_cfg.ymd_hm
    ymd = str(ymd_hm).split("_")[0]
    hm = str(ymd_hm).split("_")[1]
    pair = in_cfg.pair

    if hm > "1000" and hm < "2200":
        if p_type == 'Aerosol':
            print 'night time,there is no available aod data...'
            return False

    create_figure(
        input_path, out_path, nav_path, p_type, res, lt_lon, lt_lat, pair, ymd, hm)

if __name__ == '__main__':
    args = sys.argv[1:]
    run_job(args)
    # main()
