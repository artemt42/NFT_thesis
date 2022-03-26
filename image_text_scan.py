# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 12:45:52 2022

@author: atishakov
"""

from PIL import Image
import pytesseract

pytesseract.pytesseract.tesseract_cmd = r'C:/Program Files/Tesseract-OCR/tesseract.exe'

def ocr_core(filename):
    text = pytesseract.image_to_string(Image.open(filename))
    return text


file_dir = 'C:/Users/atish/Google Drive/BIM Thesis/NFT data/OpenSea.io/api_form_2.png'
print(ocr_core(file_dir))
