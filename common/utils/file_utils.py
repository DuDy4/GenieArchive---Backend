import os
import docx
import fitz  # PyMuPDF for PDF
from pptx import Presentation

def get_file_name_from_url(url: str) -> str:
    return os.path.basename(url)

def get_file_extension(file_name: str) -> str:
    return os.path.splitext(file_name)[1]
# PDF extraction (using PyMuPDF)
def extract_text_from_pdf(file_content):
    pdf_document = fitz.open(stream=file_content, filetype="pdf")
    text = ""
    for page in pdf_document:
        text += page.get_text("text")
    return text

# DOCX extraction
def extract_text_from_docx(file_content):
    doc = docx.Document(file_content)
    text = "\n".join([paragraph.text for paragraph in doc.paragraphs])
    return text

# PPTX extraction
def extract_text_from_pptx(file_content):
    presentation = Presentation(file_content)
    text = ""
    for slide in presentation.slides:
        for shape in slide.shapes:
            if hasattr(shape, "text"):
                text += shape.text + "\n"
    return text
