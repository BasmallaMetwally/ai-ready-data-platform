"""
exceptions.py
-------------
Exceptions مخصصة للـ ETL Pipeline عشان نقدر نميّز نوع الخطأ ونتعامل
معاه بشكل مختلف (retry, skip, fail entirely) في مكان واحد.
"""


class PipelineError(Exception):
    """الأب لكل أخطاء الـ pipeline."""


class ExtractionError(PipelineError):
    """خطأ أثناء قراءة/استخراج البيانات من المصدر."""


class DataValidationError(PipelineError):
    """خطأ لما البيانات تفشل في اختبارات الجودة الحرجة (critical checks)."""

    def __init__(self, message: str, failed_checks: list = None):
        super().__init__(message)
        self.failed_checks = failed_checks or []


class LoadError(PipelineError):
    """خطأ أثناء تحميل البيانات في قاعدة البيانات."""
