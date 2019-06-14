# coding: UTF-8

'''
Created on 2018-05-17
@author: yanguoqing

calculte rayleigh scatter 
@modified on 2018-05-20
change the method for calculating the scale of  rayleigh
'''
##pyspectral --rayleigh correction
import os,shutil,sys
import numpy as np
import h5py

bandnames={
    'B01':'ch1',
    'B02':'ch2',
    'B03':'ch3',
    'B04':'ch4'
}

def get_rayleigh_scatter(bandArr,bandname,sun_zenith,sat_zenith,azimuth_diff):
    
    #get channel name 
    channel=bandnames[bandname]

    # pysprctral package
    from pyspectral.rayleigh import Rayleigh
   
## from pyspectral.utils import debug_on
    ## check the processing info for test
   ## debug_on()

    ## set atmosphere type and aerosoltype parameters
    ## set sensor info
    '''
    AEROSOL_TYPES = ['antarctic_aerosol', 'continental_average_aerosol','continental_clean_aerosol', 'continental_polluted_aerosol',
                    'desert_aerosol', 'marine_clean_aerosol',
                    'marine_polluted_aerosol', 'marine_tropical_aerosol',
                    'rayleigh_only', 'rural_aerosol', 'urban_aerosol']

    ATMOSPHERES = {'subarctic summer': 4, 'subarctic winter': 5,
                'midlatitude summer': 6, 'midlatitude winter': 7,
                'tropical': 8, 'us-standard': 9}
    '''

    
    ahi8Rayleigh=Rayleigh('Himawari-8','ahi',atmosphere='midlatitude summer',aerosol_type='rayleigh_only')
    ##get reflectance == calculate rayleigh scattering contributor
    ref_correct= ahi8Rayleigh.get_reflectance(sun_zenith,sat_zenith,azimuth_diff,channel)

    #To avoid overcorrection of pixels at high satellite zenith angles,the Rayleigh component was reduced by a factor scaling 
    scaleRayleigh=get_rayleigh_scale(sun_zenith)

    ref_correct *= scaleRayleigh
    scaleRayleigh=0
    mmax=np.max(bandArr)
    bandArr=bandArr*100
    bandArr -= ref_correct
    bandArr *=0.01
    #ref_correct = ref_correct.astype(np.uint16)
    #mmax=np.max(bandArr)
    #bandArr -= ref_correct
    #bandArr=bandArr.astype(np.uint16)
    bandArr[bandArr>mmax]=0
    return bandArr

#To avoid overcorrection of pixels at high satellite zenith angles,the Rayleigh component was reduced by a factor scaling 
#The correction is limited to *limit* degrees (default: 88.0 degrees). Forlarger zenith angles, the correction is the same as at the *limit*.
#another correction formula: 24.35 / (2. * cos_zen +np.sqrt(498.5225 * cos_zen**2 + 1)),cos_zen is the cosine of the zenith angle,and 
#This function uses the correction method proposed byLi and Shibata (2006): https://doi.org/10.1175/JAS3682.1
def get_rayleigh_scale(soz):
    
    soz[soz>88]=88
    soz[soz<-88]=-88
    scaleSoz=np.cos(np.deg2rad(soz))
    return scaleSoz

if __name__ == '__main__':

    print 'calculte the rayleigh scattering contributor to shortwave channel!'

