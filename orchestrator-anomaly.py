import subprocess
import threading
import time

def run_producer():
    """Run the data producer script."""
    subprocess.run(['python', 'data_producer.py'])

def run_consumer():
    """Run the data consumer script."""
    subprocess.run(['python', 'data_consumer.py'])

def main():
    # Create threads for producer and consumer
    producer_thread = threading.Thread(target=run_producer, daemon=True)
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    
    # Start threads
    producer_thread.start()
    consumer_thread.start()
    
    print("Orchestrator started. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Orchestrator stopped.")

if __name__ == "__main__":
    main()
