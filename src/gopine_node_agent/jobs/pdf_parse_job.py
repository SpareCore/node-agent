"""
PDF Parse Job

Implementation of PDF parsing job for the GoPine system.
"""

import json
import logging
import os
import subprocess
import time
from typing import Dict, Any, List, Optional, Tuple

import PyPDF2
import tabula
import camelot

from gopine_node_agent.jobs.base_job import BaseJob

logger = logging.getLogger(__name__)

class PDFParseJob(BaseJob):
    """
    PDF parsing job implementation.
    
    Extracts text, tables, forms, and metadata from PDF documents.
    """
    
    def __init__(self, job_id: str, job_data: Dict[str, Any], work_dir: str):
        """
        Initialize a PDF parsing job.
        
        Args:
            job_id (str): Unique identifier for the job
            job_data (Dict[str, Any]): Job data from the server
            work_dir (str): Working directory for this job
        """
        super().__init__(job_id, job_data, work_dir)
        
        # Extract PDF parsing specific parameters
        self.extraction_tasks = self.parameters.get("extraction_tasks", [])
        self.output_format = self.parameters.get("output_format", "json")
        self.password = self.parameters.get("password", None)
        
        # Task-specific options
        self.table_options = self.parameters.get("table_extraction_options", {})
        self.text_options = self.parameters.get("text_extraction_options", {})
        self.form_options = self.parameters.get("form_extraction_options", {})
        
        # Results tracking
        self.pages_processed = 0
        self.task_results = {}
        
        # Output files
        self.output_files = {}
    
    def _validate_parameters(self):
        """
        Validate PDF parsing job parameters.
        
        Raises:
            ValueError: If parameters are invalid
        """
        # Check input file
        input_file = self.parameters.get("input_file")
        if not input_file:
            raise ValueError("Input file is required for PDF parsing job")
        
        # Check file extension
        ext = os.path.splitext(input_file.lower())[1]
        if ext != ".pdf":
            raise ValueError("Input file must be a PDF document")
        
        # Check extraction tasks
        if not self.extraction_tasks:
            raise ValueError("At least one extraction task must be specified")
        
        # Validate task types
        valid_tasks = {"text", "tables", "forms", "images", "metadata", "structure"}
        for task in self.extraction_tasks:
            task_type = task.get("task_type")
            if not task_type:
                raise ValueError("Missing task_type in extraction task")
            if task_type not in valid_tasks:
                raise ValueError(f"Invalid task type: {task_type}")
    
    def _process_job(self) -> Dict[str, Any]:
        """
        Process the PDF parsing job.
        
        Returns:
            Dict[str, Any]: Job result data
        """
        logger.info("Processing PDF parsing job %s", self.job_id)
        
        # Get input file path
        input_file = self.parameters.get("input_file")
        
        # Open the PDF
        pdf = self._open_pdf(input_file)
        
        # Process each task
        for task in self.extraction_tasks:
            task_type = task.get("task_type")
            page_range = task.get("page_range", "all")
            
            # Convert page range to list of page numbers
            pages = self._parse_page_range(page_range, pdf.getNumPages())
            
            try:
                if task_type == "text":
                    self._extract_text(pdf, pages)
                elif task_type == "tables":
                    self._extract_tables(input_file, pages)
                elif task_type == "metadata":
                    self._extract_metadata(pdf)
                elif task_type == "forms":
                    self._extract_forms(pdf, pages)
                elif task_type == "images":
                    self._extract_images(input_file, pages)
                elif task_type == "structure":
                    self._extract_structure(pdf, pages)
            except Exception as e:
                logger.error("Error in task %s: %s", task_type, str(e), exc_info=True)
                self.task_results[task_type] = {"error": str(e)}
        
        # Create combined output file if needed
        if self.output_format == "json":
            self._create_combined_output()
        
        # Return the result
        return {
            "output_files": self.output_files,
            "pages_processed": self.pages_processed,
            "processing_time_seconds": (time.time() - self.start_time.timestamp()),
            "metadata": self.task_results.get("metadata", {}),
            "task_results": self.task_results
        }
    
    def _open_pdf(self, pdf_path: str) -> PyPDF2.PdfReader:
        """
        Open a PDF file and return a PdfReader object.
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            PyPDF2.PdfReader: PDF reader object
        """
        try:
            pdf = PyPDF2.PdfReader(pdf_path)
            
            # Handle encrypted PDFs
            if pdf.is_encrypted and self.password:
                pdf.decrypt(self.password)
            
            return pdf
        except Exception as e:
            logger.error("Error opening PDF: %s", str(e), exc_info=True)
            raise
    
    def _parse_page_range(self, page_range: str, total_pages: int) -> List[int]:
        """
        Parse a page range string into a list of page numbers.
        
        Args:
            page_range (str): Page range string (e.g., "1-5", "1,3,5", "all")
            total_pages (int): Total number of pages in the PDF
            
        Returns:
            List[int]: List of page numbers (0-indexed)
        """
        if page_range == "all":
            return list(range(total_pages))
        
        pages = []
        parts = page_range.split(",")
        
        for part in parts:
            if "-" in part:
                start, end = part.split("-")
                start = int(start) - 1  # Convert to 0-indexed
                end = int(end)  # End is exclusive, so no need to subtract 1
                pages.extend(range(start, end))
            else:
                pages.append(int(part) - 1)  # Convert to 0-indexed
        
        # Filter out invalid page numbers
        pages = [p for p in pages if 0 <= p < total_pages]
        
        return pages
    
    def _extract_text(self, pdf: PyPDF2.PdfReader, pages: List[int]):
        """
        Extract text from PDF pages.
        
        Args:
            pdf (PyPDF2.PdfReader): PDF reader object
            pages (List[int]): List of page numbers to extract
        """
        logger.info("Extracting text from %d pages", len(pages))
        
        preserve_formatting = self.text_options.get("preserve_formatting", True)
        include_line_breaks = self.text_options.get("include_line_breaks", True)
        
        extracted_text = ""
        character_count = 0
        word_count = 0
        
        for i, page_num in enumerate(pages):
            self.update_progress((i / len(pages)) * 100)
            
            try:
                page = pdf.pages[page_num]
                
                # Extract text from the page
                page_text = page.extract_text()
                
                # Process text based on options
                if not preserve_formatting:
                    # Remove extra whitespace
                    page_text = " ".join(page_text.split())
                
                if not include_line_breaks:
                    # Replace line breaks with spaces
                    page_text = page_text.replace("\n", " ")
                
                # Add page separator
                if extracted_text:
                    extracted_text += "\n\n----- Page " + str(page_num + 1) + " -----\n\n"
                else:
                    extracted_text += "----- Page " + str(page_num + 1) + " -----\n\n"
                
                extracted_text += page_text
                
                # Count characters and words
                character_count += len(page_text)
                word_count += len(page_text.split())
                
                self.pages_processed += 1
                
            except Exception as e:
                logger.error("Error extracting text from page %d: %s", page_num + 1, str(e))
        
        # Save to output file
        output_path = os.path.join(self.output_dir, "extracted_text.txt")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(extracted_text)
        
        # Record results
        self.output_files["text"] = output_path
        self.task_results["text"] = {
            "character_count": character_count,
            "word_count": word_count
        }
    
    def _extract_tables(self, pdf_path: str, pages: List[int]):
        """
        Extract tables from PDF pages.
        
        Args:
            pdf_path (str): Path to the PDF file
            pages (List[int]): List of page numbers to extract
        """
        logger.info("Extracting tables from %d pages", len(pages))
        
        algorithm = self.table_options.get("algorithm", "lattice")
        include_headers = self.table_options.get("include_headers", True)
        
        # Convert 0-indexed to 1-indexed for tabula and camelot
        pages_1_indexed = [p + 1 for p in pages]
        
        tables_extracted = 0
        
        try:
            if algorithm == "lattice":
                # Use camelot for lattice tables (tables with borders)
                tables = camelot.read_pdf(
                    pdf_path,
                    pages=",".join(map(str, pages_1_indexed)),
                    flavor="lattice"
                )
                tables_extracted = len(tables)
                
                # Save tables to CSV files
                for i, table in enumerate(tables):
                    output_path = os.path.join(self.output_dir, f"table_{i+1}.csv")
                    table.to_csv(output_path)
                
            elif algorithm == "stream":
                # Use tabula for stream tables (tables without borders)
                tables = tabula.read_pdf(
                    pdf_path,
                    pages=",".join(map(str, pages_1_indexed)),
                    multiple_tables=True
                )
                tables_extracted = len(tables)
                
                # Save tables to CSV files
                for i, table in enumerate(tables):
                    output_path = os.path.join(self.output_dir, f"table_{i+1}.csv")
                    table.to_csv(output_path, index=False)
            
            else:
                # Default to combined approach
                lattice_tables = camelot.read_pdf(
                    pdf_path,
                    pages=",".join(map(str, pages_1_indexed)),
                    flavor="lattice"
                )
                
                stream_tables = camelot.read_pdf(
                    pdf_path,
                    pages=",".join(map(str, pages_1_indexed)),
                    flavor="stream"
                )
                
                tables_extracted = len(lattice_tables) + len(stream_tables)
                
                # Save tables to CSV files
                for i, table in enumerate(lattice_tables):
                    output_path = os.path.join(self.output_dir, f"lattice_table_{i+1}.csv")
                    table.to_csv(output_path)
                
                for i, table in enumerate(stream_tables):
                    output_path = os.path.join(self.output_dir, f"stream_table_{i+1}.csv")
                    table.to_csv(output_path)
            
            # Create combined JSON file for tables
            tables_json_path = os.path.join(self.output_dir, "tables.json")
            tables_info = {
                "total_tables": tables_extracted,
                "pages_processed": len(pages),
                "algorithm": algorithm
            }
            
            with open(tables_json_path, "w", encoding="utf-8") as f:
                json.dump(tables_info, f, indent=2)
            
            # Record results
            self.output_files["tables"] = os.path.join(self.output_dir, "tables")
            self.task_results["tables"] = {
                "tables_extracted": tables_extracted
            }
            
            self.pages_processed += len(pages)
            
        except Exception as e:
            logger.error("Error extracting tables: %s", str(e), exc_info=True)
            raise
    
    def _extract_metadata(self, pdf: PyPDF2.PdfReader):
        """
        Extract metadata from the PDF.
        
        Args:
            pdf (PyPDF2.PdfReader): PDF reader object
        """
        logger.info("Extracting metadata from PDF")
        
        try:
            # Get document info dictionary
            info = pdf.metadata
            
            # Convert to Python dictionary (info is a read-only dictionary)
            metadata = {}
            if info:
                for key, value in info.items():
                    # Remove leading slash from keys
                    clean_key = key
                    if clean_key.startswith("/"):
                        clean_key = clean_key[1:]
                    metadata[clean_key] = str(value)
            
            # Add basic PDF information
            metadata["page_count"] = len(pdf.pages)
            
            # Save to output file
            output_path = os.path.join(self.output_dir, "metadata.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)
            
            # Record results
            self.output_files["metadata"] = output_path
            self.task_results["metadata"] = metadata
            
        except Exception as e:
            logger.error("Error extracting metadata: %s", str(e), exc_info=True)
            raise
    
    def _extract_forms(self, pdf: PyPDF2.PdfReader, pages: List[int]):
        """
        Extract form fields from PDF pages.
        
        Args:
            pdf (PyPDF2.PdfReader): PDF reader object
            pages (List[int]): List of page numbers to extract
        """
        logger.info("Extracting form fields")
        
        include_field_properties = self.form_options.get("include_field_properties", True)
        
        try:
            # Get form fields
            fields = pdf.get_form_text_fields()
            
            if include_field_properties:
                # This would require more detailed form field extraction
                # For now, just use the basic fields
                pass
            
            # Save to output file
            output_path = os.path.join(self.output_dir, "form_fields.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(fields, f, indent=2)
            
            # Record results
            self.output_files["forms"] = output_path
            self.task_results["forms"] = {
                "fields_extracted": len(fields)
            }
            
        except Exception as e:
            logger.error("Error extracting form fields: %s", str(e), exc_info=True)
            self.task_results["forms"] = {"error": str(e)}
    
    def _extract_images(self, pdf_path: str, pages: List[int]):
        """
        Extract images from PDF pages.
        
        Args:
            pdf_path (str): Path to the PDF file
            pages (List[int]): List of page numbers to extract
        """
        logger.info("Extracting images from %d pages", len(pages))
        
        # Create images directory
        images_dir = os.path.join(self.output_dir, "images")
        os.makedirs(images_dir, exist_ok=True)
        
        images_extracted = 0
        
        try:
            # This is a simplified implementation
            # In a real implementation, we would use pdfimages (from poppler-utils)
            # or a Python library like PyMuPDF (fitz) to extract images
            
            # For demonstration, just create a sample image info file
            image_info = {
                "message": "Image extraction would be implemented here",
                "pages_processed": len(pages)
            }
            
            # Save to output file
            output_path = os.path.join(self.output_dir, "images_info.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(image_info, f, indent=2)
            
            # Record results
            self.output_files["images"] = images_dir
            self.task_results["images"] = {
                "images_extracted": images_extracted
            }
            
            self.pages_processed += len(pages)
            
        except Exception as e:
            logger.error("Error extracting images: %s", str(e), exc_info=True)
            self.task_results["images"] = {"error": str(e)}
    
    def _extract_structure(self, pdf: PyPDF2.PdfReader, pages: List[int]):
        """
        Extract document structure from PDF pages.
        
        Args:
            pdf (PyPDF2.PdfReader): PDF reader object
            pages (List[int]): List of page numbers to extract
        """
        logger.info("Extracting document structure from %d pages", len(pages))
        
        try:
            # Get document outline (bookmarks)
            outline = self._extract_outline(pdf)
            
            # Basic page structure information
            page_info = []
            for page_num in pages:
                if page_num < len(pdf.pages):
                    page = pdf.pages[page_num]
                    
                    # Basic page information
                    page_info.append({
                        "page_number": page_num + 1,
                        "rotation": page.get("/Rotate", 0),
                        "media_box": self._get_rect_values(page.mediabox),
                        "crop_box": self._get_rect_values(page.cropbox)
                    })
            
            # Create structure info
            structure_info = {
                "outline": outline,
                "pages": page_info
            }
            
            # Save to output file
            output_path = os.path.join(self.output_dir, "structure.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(structure_info, f, indent=2)
            
            # Record results
            self.output_files["structure"] = output_path
            self.pages_processed += len(pages)
            
        except Exception as e:
            logger.error("Error extracting structure: %s", str(e), exc_info=True)
            raise
    
    def _extract_outline(self, pdf: PyPDF2.PdfReader) -> List[Dict]:
        """
        Extract document outline (bookmarks) from PDF.
        
        Args:
            pdf (PyPDF2.PdfReader): PDF reader object
            
        Returns:
            List[Dict]: Document outline
        """
        # This is a simplified implementation
        # PyPDF2's outline extraction is limited
        # In a real implementation, we would use a more robust library
        
        try:
            outline = pdf.outline
            return outline
        except Exception:
            return []
    
    def _get_rect_values(self, rect) -> Tuple[float, float, float, float]:
        """
        Get rectangle values from a PDF rectangle object.
        
        Args:
            rect: PDF rectangle object
            
        Returns:
            Tuple[float, float, float, float]: Rectangle values (x1, y1, x2, y2)
        """
        if hasattr(rect, "as_list"):
            return tuple(rect.as_list())
        return (0, 0, 0, 0)
    
    def _create_combined_output(self):
        """Create a combined output file with all results."""
        combined_output = {
            "job_id": self.job_id,
            "output_files": self.output_files,
            "task_results": self.task_results,
            "pages_processed": self.pages_processed,
            "processing_time_seconds": (time.time() - self.start_time.timestamp())
        }
        
        # Add metadata if available
        if "metadata" in self.task_results:
            combined_output["metadata"] = self.task_results["metadata"]
        
        # Save to output file
        output_path = os.path.join(self.output_dir, "combined_results.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(combined_output, f, indent=2)
        
        self.output_files["combined"] = output_path