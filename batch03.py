
import logging
import sqlite3

# ==========================================
# ۱. پیکربندی سیستم لاگینگ (Logging Setup)
# ==========================================
# در پروژه‌های واقعی، لاگ‌ها با فرمت دقیق و مشخصات زمانی ذخیره می‌شوند.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | [%(levelname)s] | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),  # ذخیره در فایل دیسک
        logging.StreamHandler(),  # نمایش همزمان در ترمینال/کنسول
    ],
)
logger = logging.getLogger("DataPipeline")


# ==========================================
# ۲. ژنراتور دسته‌بندی داده‌ها (Batch Generator)
# ==========================================
def make_batches(dataset, batch_size):
    """داده‌ها را بدون پر کردن حافظه RAM به تکه‌های کوچکتری (Batch) تبدیل می‌کند."""
    for i in range(0, len(dataset), batch_size):
        yield dataset[i : i + batch_size]


# ==========================================
# ۳. تابع درج دسته‌ای (Batch Insertion)
# ==========================================
def batch_insert_transactions(conn, transactions, batch_size=1000):
    """درج دسته‌ای داده‌ها درون دیتابیس با مدیریت ایزوله هر دسته.

    - اگر یک دسته خطای داده‌ای داشته باشد، فقط همان دسته رول‌بک می‌شود و
    پایپ‌لاین متوقف نمی‌شود.
    """
    cursor = conn.cursor()
    total_inserted = 0
    batch_counter = 0

    for batch in make_batches(transactions, batch_size):
        batch_counter += 1
        try:
            # استفاده از استاندارد رسمی Context Manager برای تراکنش
            # به صورت خودکار در پایان بلاک commit و در صورت بروز خطا rollback می‌کند.
            with conn:
                cursor.executemany(
                    "INSERT INTO transactions (id, amount) VALUES (?, ?)", batch
                )

            total_inserted += len(batch)
            logger.info(
                f"Batch {batch_counter} inserted successfully ({len(batch)} records)."
            )

        except sqlite3.IntegrityError as err:
            # خطای سطح داده (Data Error) مثل نقض کلید اصلی، فیلد تکراری یا نامعتبر
            # توجه: تراکنش این دسته توسط Context Manager به طور خودکار رول‌بک شده است.
            logger.warning(
                f"Batch {batch_counter} skipped due to IntegrityError: {err}"
            )

    return total_inserted


# ==========================================
# ۴. لایه کنترلر اصلی پایپ‌لاین (Pipeline Runner)
# ==========================================
def run_pipeline(db_path, data):
    """مدیریت اتصال به پایگاه داده و کنترل خطاهای زیرساختی و کلی."""
    logger.info("Pipeline execution started.")
    conn = None

    try:
        # برقراری اتصال
        conn = sqlite3.connect(db_path)
        logger.info(f"Connected to database: '{db_path}'")

        # اجرای عملیات درج
        records_count = batch_insert_transactions(conn, data, batch_size=2)
        logger.info(
            f"Pipeline finished successfully. Total records inserted: {records_count}"
        )

    except sqlite3.OperationalError as err:
        # خطای سطح زیرساخت (Infrastructure Error): قفل بودن دیتابیس، قطع اتصال، عدم دسترسی دیسک
        logger.critical(
            f"Infrastructure failure: Database is unreachable or locked! Error: {err}"
        )

    except Exception as err:
        # سایر خطاهای پیش‌بینی‌نشده سیستمی
        logger.exception(
            f"Unexpected system crash during pipeline execution: {err}"
        )

    finally:
        # بستن امن اتصال به دیتابیس در هر شرایطی
        if conn:
            conn.close()
            logger.info("Database connection closed cleanly.")


# ==========================================
# تست عملی کد (Verification)
# ==========================================
if __name__ == "__main__":
    # ایجاد ساختار تستی جدول
    setup_conn = sqlite3.connect("safe_pipeline.db")
    setup_conn.execute(
        "CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, amount REAL)"
    )
    setup_conn.close()

    # داده‌های تست:
    # دسته ۱: داده‌های معتبر
    # دسته ۲: شامل شناسه تکراری (id=2) که باعث خطای داده‌ای می‌شود
    # دسته ۳: داده‌های معتبر
    sample_data = [
        (1, 150.0),
        (2, 200.0),  # دسته ۱
        (2, 50.0),
        (3, 75.0),  # دسته ۲ (خطا به دلیل شناسه تکراری 2)
        (4, 300.0),
        (5, 450.0),  # دسته ۳
    ]

    run_pipeline("safe_pipeline.db", sample_data)