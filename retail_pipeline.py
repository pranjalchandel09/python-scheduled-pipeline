import logging
import time

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("Pipeline started")
    time.sleep(5)
    logging.info("Pipeline finished successfully")

if __name__ == "__main__":
    main()
