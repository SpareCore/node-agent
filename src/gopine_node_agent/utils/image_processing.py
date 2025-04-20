"""
Image Processing Utilities

Utilities for preprocessing images before OCR.
"""

import logging
import math
import numpy as np
from typing import Optional

from PIL import Image, ImageFilter, ImageEnhance

logger = logging.getLogger(__name__)

def preprocess_image(
    image_path: str,
    grayscale: bool = True,
    denoise: bool = False,
    deskew: bool = True,
    contrast_enhance: bool = False
) -> Image.Image:
    """
    Preprocess an image for OCR.
    
    Args:
        image_path (str): Path to the image file
        grayscale (bool): Convert image to grayscale
        denoise (bool): Apply denoising filter
        deskew (bool): Automatically deskew (straighten) the image
        contrast_enhance (bool): Enhance image contrast
        
    Returns:
        Image.Image: Preprocessed image
    """
    try:
        # Open the image
        image = Image.open(image_path)
        
        # Convert to RGB mode if it's not already (handles RGBA, palette images, etc.)
        if image.mode != 'RGB':
            image = image.convert('RGB')
        
        # Apply grayscale conversion
        if grayscale:
            image = image.convert('L')
        
        # Apply denoising if requested
        if denoise:
            image = image.filter(ImageFilter.MedianFilter(size=3))
        
        # Apply deskewing if requested
        if deskew:
            image = _deskew_image(image)
        
        # Enhance contrast if requested
        if contrast_enhance:
            if image.mode == 'L':
                # For grayscale images
                enhancer = ImageEnhance.Contrast(image)
                image = enhancer.enhance(2.0)  # Enhance contrast by factor of 2
            else:
                # For color images
                enhancer = ImageEnhance.Contrast(image)
                image = enhancer.enhance(1.5)  # Enhance contrast by factor of 1.5
        
        return image
        
    except Exception as e:
        logger.error("Error preprocessing image %s: %s", image_path, str(e), exc_info=True)
        # Return the original image if processing fails
        return Image.open(image_path)

def _deskew_image(image: Image.Image, max_skew_angle: float = 10.0) -> Image.Image:
    """
    Deskew (straighten) an image.
    
    Args:
        image (Image.Image): Input image
        max_skew_angle (float): Maximum skew angle to correct (degrees)
        
    Returns:
        Image.Image: Deskewed image
    """
    try:
        # Convert to numpy array
        img_array = np.array(image)
        
        # This is a simplified implementation of deskewing
        # A real implementation would detect the skew angle using techniques like:
        # - Hough Line Transform
        # - Projection Profile Analysis
        
        # For this demo, we'll implement a simple version based on horizontal projections
        if image.mode == 'L':  # Grayscale
            # Threshold the image (convert to binary)
            _, binary = cv2.threshold(img_array, 128, 255, cv2.THRESH_BINARY_INV)
            
            # Find all non-zero points
            coords = np.column_stack(np.where(binary > 0))
            
            if len(coords) == 0:
                return image  # No text detected
            
            # Find the minimum area rectangle
            rect = cv2.minAreaRect(coords)
            angle = rect[2]
            
            # The angle is between -90 and 0 degrees
            # Convert to the angle between -45 and 45 degrees
            if angle < -45:
                angle = 90 + angle
            
            # Limit to max_skew_angle
            angle = max(min(angle, max_skew_angle), -max_skew_angle)
            
            # Rotate the image to correct the skew
            return image.rotate(angle, resample=Image.BICUBIC, expand=True)
        
        # For non-grayscale images, just return the original
        return image
        
    except Exception as e:
        logger.error("Error deskewing image: %s", str(e), exc_info=True)
        # Return the original image if deskewing fails
        return image

def _create_cv2_fallback():
    """Create a fallback for cv2 functions used in deskewing."""
    global cv2
    
    class CV2Fallback:
        """Fallback for cv2 functions."""
        
        @staticmethod
        def threshold(img_array, thresh, maxval, type_):
            """Simple thresholding."""
            result = np.zeros_like(img_array)
            if type_ == 1:  # THRESH_BINARY_INV
                result[img_array < thresh] = maxval
            else:
                result[img_array >= thresh] = maxval
            return None, result
        
        @staticmethod
        def minAreaRect(points):
            """Find minimum area rectangle (simplified)."""
            x, y = zip(*points)
            return ((np.mean(x), np.mean(y)), (0, 0), 0)

    cv2 = CV2Fallback()

# Try to import OpenCV, or use fallback
try:
    import cv2
except ImportError:
    logger.warning("OpenCV not available, using simplified implementation for deskewing")
    _create_cv2_fallback()