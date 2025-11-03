"""
Main Pipeline Runner

Runs both pipelines concurrently:
1. categorySearchPipeline.py - Discovers e-commerce sites and saves search URL templates
2. productExtractionPipeline.py - Extracts products using saved templates

Both pipelines run endlessly in parallel threads.
"""

import sys
import os
import threading
import time
import signal
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import pipelines
from LaunchPad.categorySearchPipeline import CategorySearchPipeline
from LaunchPad.productExtractionPipeline import ProductExtractionPipeline


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks"""
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = b'{"status":"ok","message":"Pipelines running"}'
            self.wfile.write(response)
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress health check logs
        pass


class PipelineManager:
    """
    Manages both pipelines running concurrently
    """
    
    def __init__(self):
        """Initialize both pipelines"""
        print(f"\n{'='*80}")
        print("INITIALIZING PIPELINE MANAGER")
        print(f"{'='*80}\n")
        
        # Initialize pipelines
        print("[*] Initializing Category Search Pipeline...")
        self.category_pipeline = CategorySearchPipeline()
        
        print("\n[*] Initializing Product Extraction Pipeline...")
        self.product_pipeline = ProductExtractionPipeline()
        
        # Threads for running pipelines
        self.category_thread: Optional[threading.Thread] = None
        self.product_thread: Optional[threading.Thread] = None
        self.health_check_thread: Optional[threading.Thread] = None
        self.health_server: Optional[HTTPServer] = None
        
        # Control flags
        self.running = False
        
        print(f"\n{'='*80}")
        print("PIPELINE MANAGER INITIALIZED")
        print(f"{'='*80}\n")
    
    def run_category_pipeline(self):
        """Run category search pipeline in a thread"""
        try:
            print(f"\n[CATEGORY PIPELINE] Starting...")
            self.category_pipeline.run_continuous(delay_between_categories=5)
        except Exception as e:
            print(f"\n[CATEGORY PIPELINE] Error: {e}")
            if self.running:
                print(f"[CATEGORY PIPELINE] Restarting in 10 seconds...")
                time.sleep(10)
                if self.running:
                    self.run_category_pipeline()
    
    def run_product_pipeline(self):
        """Run product extraction pipeline in a thread"""
        try:
            print(f"\n[PRODUCT PIPELINE] Starting...")
            self.product_pipeline.run_continuous(delay_between_products=2)
        except Exception as e:
            print(f"\n[PRODUCT PIPELINE] Error: {e}")
            if self.running:
                print(f"[PRODUCT PIPELINE] Restarting in 10 seconds...")
                time.sleep(10)
                if self.running:
                    self.run_product_pipeline()
    
    def run_health_check_server(self):
        """Run HTTP health check server for Railway"""
        try:
            port = int(os.getenv("PORT", "8080"))
            self.health_server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
            print(f"[*] Health check server started on port {port}")
            print(f"[*] Health endpoint: http://0.0.0.0:{port}/health")
            self.health_server.serve_forever()
        except Exception as e:
            print(f"\n[HEALTH SERVER] Error: {e}")
            if self.running:
                time.sleep(5)
                if self.running:
                    self.run_health_check_server()
    
    def start(self):
        """Start both pipelines in separate threads"""
        print(f"\n{'='*80}")
        print("STARTING ALL PIPELINES")
        print(f"{'='*80}\n")
        print("[*] Category Search Pipeline: Discovering e-commerce sites and templates")
        print("[*] Product Extraction Pipeline: Extracting products using templates")
        print("[*] Both pipelines will run concurrently\n")
        
        self.running = True
        
        # Start category pipeline thread
        self.category_thread = threading.Thread(
            target=self.run_category_pipeline,
            name="CategoryPipeline",
            daemon=True
        )
        self.category_thread.start()
        print("[✓] Category Search Pipeline thread started")
        
        # Small delay before starting second pipeline
        time.sleep(2)
        
        # Start product extraction pipeline thread
        self.product_thread = threading.Thread(
            target=self.run_product_pipeline,
            name="ProductExtractionPipeline",
            daemon=True
        )
        self.product_thread.start()
        print("[✓] Product Extraction Pipeline thread started")
        
        # Start health check server thread (for Railway)
        self.health_check_thread = threading.Thread(
            target=self.run_health_check_server,
            name="HealthCheckServer",
            daemon=True
        )
        self.health_check_thread.start()
        print("[✓] Health check server started\n")
        
        print(f"{'='*80}")
        print("ALL PIPELINES RUNNING")
        print(f"{'='*80}")
        print("\n[INFO] Both pipelines are running concurrently:")
        print("  → Category Pipeline: Discovers new e-commerce sites & templates")
        print("  → Product Pipeline: Extracts products using saved templates")
        print("  → Health Check: HTTP server responding on /health endpoint")
        print("\n[INFO] Press Ctrl+C to stop all pipelines\n")
        
        # Wait for threads (they run continuously)
        try:
            while self.running:
                # Check if threads are still alive
                if self.category_thread and not self.category_thread.is_alive():
                    print("\n[!] Category Pipeline thread died, restarting...")
                    self.category_thread = threading.Thread(
                        target=self.run_category_pipeline,
                        name="CategoryPipeline",
                        daemon=True
                    )
                    self.category_thread.start()
                
                if self.product_thread and not self.product_thread.is_alive():
                    print("\n[!] Product Pipeline thread died, restarting...")
                    self.product_thread = threading.Thread(
                        target=self.run_product_pipeline,
                        name="ProductExtractionPipeline",
                        daemon=True
                    )
                    self.product_thread.start()
                
                if self.health_check_thread and not self.health_check_thread.is_alive():
                    print("\n[!] Health check server thread died, restarting...")
                    self.health_check_thread = threading.Thread(
                        target=self.run_health_check_server,
                        name="HealthCheckServer",
                        daemon=True
                    )
                    self.health_check_thread.start()
                
                time.sleep(5)  # Check every 5 seconds
                
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop all pipelines"""
        print(f"\n\n{'='*80}")
        print("STOPPING ALL PIPELINES")
        print(f"{'='*80}\n")
        
        self.running = False
        
        # Stop health check server
        if self.health_server:
            print("[*] Stopping health check server...")
            self.health_server.shutdown()
        
        print("[*] Waiting for pipelines to finish current operations...")
        time.sleep(2)
        
        print("[✓] All pipelines stopped")
        print(f"{'='*80}\n")


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n\n[!] Interrupt signal received")
    if 'manager' in globals():
        manager.stop()
    sys.exit(0)


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print(f"\n{'='*80}")
    print("MAIN PIPELINE RUNNER")
    print(f"{'='*80}")
    print("\nThis script runs both pipelines concurrently:")
    print("  1. Category Search Pipeline - Discovers e-commerce sites")
    print("  2. Product Extraction Pipeline - Extracts products")
    print(f"{'='*80}\n")
    
    # Create and start pipeline manager
    manager = PipelineManager()
    
    try:
        manager.start()
    except Exception as e:
        print(f"\n[✗] Fatal error: {e}")
        manager.stop()
        sys.exit(1)

