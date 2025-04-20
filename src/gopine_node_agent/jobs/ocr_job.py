"""
OCR Job

Implementation of OCR (Optical Character Recognition) job for the GoPine system.
"""

import logging
import os
import shutil
import subprocess
import time
from typing import Dict, Any, List, Optional

import pytesseract
from PIL import Image

from gopine_node_agent.jobs.base_job import BaseJob
from gopine_node_agent.utils.image_processing import preprocess_image

logger = logging.getLogger(__name__)

class OCRJob(BaseJob):
    """
    OCR job implementation.
    
    Processes images or PDFs to extract text using OCR technology.
    """
    
    def __init__(self, job_id: str, job_data: Dict[str, Any], work_dir: str):
        """
        Initialize an OCR job.
        
        Args:
            job_id (str): Unique identifier for the job
            job_data (Dict[str, Any]): Job data from the server
            work_dir (str): Working directory for this job
        """
        super().__init__(job_id, job_data, work_dir)
        
        # OCR-specific directories
        self.pages_dir = os.path.join(self.work_dir, "pages")
        os.makedirs(self.pages_dir, exist_ok=True)
        
        # Extract OCR-specific parameters
        self.language = self.parameters.get("language", "eng")
        self.output_format = self.parameters.get("output_format", "txt")
        self.dpi = self.parameters.get("dpi", 300)
        self.page_range = self.parameters.get("page_range", "all")
        
        # Preprocessing options
        self.preprocessing = self.parameters.get("preprocessing", {})
        self.grayscale = self.preprocessing.get("grayscale", True)
        self.denoise = self.preprocessing.get("denoise", False)
        self.deskew = self.preprocessing.get("deskew", True)
        self.contrast_enhance = self.preprocessing.get("contrast_enhance", False)
        
        # Advanced options
        self.advanced_options = self.parameters.get("advanced_options", {})
        self.engine = self.advanced_options.get("engine", "tesseract")
        self.psm = self.advanced_options.get("psm", 3)  # Page segmentation mode
        self.oem = self.advanced_options.get("oem", 3)  # OCR Engine mode
        
        # Results tracking
        self.pages_processed = 0
        self.characters_recognized = 0
        self.overall_confidence = 0.0
        self.page_details = []
    
    def _validate_parameters(self):
        """
        Validate OCR job parameters.
        
        Raises:
            ValueError: If parameters are invalid
        """
        # Check input file
        input_file = self.parameters.get("input_file")
        if not input_file:
            raise ValueError("Input file is required for OCR job")
            
        # Validate language
        valid_langs = {"eng", "fra", "deu", "spa", "ita", "por", "chi_sim", "chi_tra", "jpn", "kor"}
        if self.language not in valid_langs:
            logger.warning("Language '%s' may not be supported by the OCR engine", self.language)
            
        # Validate output format
        valid_formats = {"txt", "json", "docx", "pdf", "csv"}
        if self.output_format not in valid_formats:
            raise ValueError(f"Invalid output format: {self.output_format}")
    
    def _process_job(self) -> Dict[str, Any]:
        """
        Process the OCR job.
        
        Returns:
            Dict[str, Any]: Job result data
        """
        logger.info("Processing OCR job %s", self.job_id)
        
        # Get input file path
        input_file = self.parameters.get("input_file")
        
        # Check file extension to determine processing approach
        ext = os.path.splitext(input_file.lower())[1]
        
        # For PDF files, extract pages first
        if ext == ".pdf":
            self._extract_pages_from_pdf(input_file)
        else:
            # For single image, just copy it to pages directory
            target_file = os.path.join(self.pages_dir, "page-001.jpg")
            shutil.copy(input_file, target_file)
        
        # Process each page with OCR
        page_files = sorted([f for f in os.listdir(self.pages_dir) if f.startswith("page-")])
        total_pages = len(page_files)
        
        logger.info("Processing %d pages for OCR", total_pages)
        
        # Initialize result containers
        all_text = ""
        page_confidences = []
        
        # Process each page
        for i, page_file in enumerate(page_files):
            page_num = i + 1
            self.update_progress((i / total_pages) * 100)
            
            try:
                # Full path to image file
                image_path = os.path.join(self.pages_dir, page_file)
                
                # Preprocess the image
                preprocessed_image = preprocess_image(
                    image_path,
                    grayscale=self.grayscale,
                    denoise=self.denoise,
                    deskew=self.deskew,
                    contrast_enhance=self.contrast_enhance
                )
                
                # Perform OCR on the preprocessed image
                logger.debug("Performing OCR on page %d", page_num)
                
                # Configure OCR options
                config = f"--psm {self.psm} --oem {self.oem}"
                
                # Extract text
                ocr_data = pytesseract.image_to_data(
                    preprocessed_image, 
                    lang=self.language,
                    output_type=pytesseract.Output.DICT,
                    config=config
                )
                
                # Extract text and confidence from OCR data
                text = " ".join([word for word in ocr_data["text"] if word.strip()])
                confidences = [conf for conf in ocr_data["conf"] if conf > 0]
                
                # Calculate page confidence
                page_confidence = sum(confidences) / len(confidences) if confidences else 0
                page_confidences.append(page_confidence)
                
                # Add page separator
                if all_text:
                    all_text += "\\n\\n----- Page " + str(page_num) + " -----\\n\\n"
                else:
                    all_text += "----- Page " + str(page_num) + " -----\\n\\n"
                
                all_text += text
                
                # Count characters
                self.characters_recognized += len(text)
                
                # Track page details
                self.page_details.append({
                    "page_number": page_num,
                    "confidence_score": page_confidence,
                    "word_count": len([w for w in ocr_data["text"] if w.strip()])
                })
                
                self.pages_processed += 1
                
            except Exception as e:
                logger.error("Error processing page %d: %s", page_num, str(e), exc_info=True)
                self.page_details.append({
                    "page_number": page_num,
                    "error": str(e)
                })
        
        # Save the combined text to output file
        output_file = self._save_output(all_text)
        
        # Calculate overall confidence
        self.overall_confidence = sum(page_confidences) / len(page_confidences) if page_confidences else 0
        
        # Return the result
        return {
            "output_file": output_file,
            "text_content": all_text[:10000] if len(all_text) <= 10000 else all_text[:10000] + "... [truncated]",
            "pages_processed": self.pages_processed,
            "characters_recognized": self.characters_recognized,
            "confidence_score": self.overall_confidence,
            "processing_time_seconds": (time.time() - self.start_time.timestamp()),
            "page_details": self.page_details
        }
    
    def _extract_pages_from_pdf(self, pdf_path: str):
        """
        Extract pages from a PDF file as images.
        
        Args:
            pdf_path (str): Path to the PDF file
        """
        logger.info("Extracting pages from PDF: %s", pdf_path)
        
        try:
            # Use poppler's pdftoppm to convert PDF pages to images
            # This requires poppler-utils to be installed
            command = [
                "pdftoppm",
                "-jpeg",
                "-r", str(self.dpi),
                pdf_path,
                os.path.join(self.pages_dir, "page")
            ]
            
            # Execute the command
            process = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            
            logger.debug("PDF extraction output: %s", process.stdout)
            
            # Check if pages were extracted
            pages = [f for f in os.listdir(self.pages_dir) if f.startswith("page-")]
            if not pages:
                raise Exception("No pages extracted from PDF")
            
            logger.info("Extracted %d pages from PDF", len(pages))
            
        except subprocess.CalledProcessError as e:
            logger.error("Error extracting pages from PDF: %s", e.stderr)
            raise Exception(f"PDF extraction failed: {e.stderr}")
        
        except Exception as e:
            logger.error("Error extracting pages from PDF: %s", str(e), exc_info=True)
            raise
    
    def _save_output(self, text: str) -> str:
        """
        Save the OCR output to a file.
        
        Args:
            text (str): Extracted text
            
        Returns:
            str: Path to the output file
        """
        # Determine output filename
        base_name = "ocr_output"
        
        if self.output_format == "txt":
            output_path = os.path.join(self.output_dir, f"{base_name}.txt")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
                
        elif self.output_format == "json":
            import json
            output_path = os.path.join(self.output_dir, f"{base_name}.json")
            data = {
                "job_id": self.job_id,
                "text": text,
                "pages": self.page_details,
                "confidence": self.overall_confidence,
                "pages_processed": self.pages_processed,
                "characters_recognized": self.characters_recognized
            }
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        
        # Other formats would be implemented here
        
        else:
            # Default to txt for now
            output_path = os.path.join(self.output_dir, f"{base_name}.txt")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
        
        logger.info("Saved OCR output to %s", output_path)
        return output_path