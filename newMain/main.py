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
import datetime
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import pipelines
from LaunchPad.categorySearchPipeline import CategorySearchPipeline
from LaunchPad.productExtractionPipeline import ProductExtractionPipeline


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks with activity tracking"""
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = f'{{"status":"ok","message":"Pipelines running","timestamp":"{datetime.datetime.now().isoformat()}"}}'.encode()
            self.wfile.write(response)
        elif self.path == '/':
            # Root endpoint also responds to keep service active
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'<html><body><h1>Pipeline Service Active</h1><p>Health check: <a href="/health">/health</a></p></body></html>')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Keep health check logs minimal but visible for debugging
        if self.path == '/health':
            print(f"[HEALTH] {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Health check from {self.address_string()}")


class PipelineManager:
    """
    Manages both pipelines running concurrently
    """
    
    def __init__(self):
        """Initialize both pipelines"""
        print(f"\n{'='*80}")
        print("INITIALIZING PIPELINE MANAGER")
        print(f"{'='*80}\n")
        
        # Get resource configuration from environment
        self.max_parallel_categories = int(os.getenv("MAX_PARALLEL_CATEGORIES", "5"))
        self.max_parallel_products = int(os.getenv("MAX_PARALLEL_PRODUCTS", "10"))
        self.max_parallel_browsers = int(os.getenv("MAX_PARALLEL_BROWSERS", "8"))
        
        print(f"[*] Resource Configuration:")
        print(f"    → Max parallel categories: {self.max_parallel_categories}")
        print(f"    → Max parallel products: {self.max_parallel_products}")
        print(f"    → Max parallel browsers: {self.max_parallel_browsers}\n")
        
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
        self.last_activity_time = time.time()
        
        print(f"\n{'='*80}")
        print("PIPELINE MANAGER INITIALIZED")
        print(f"{'='*80}\n")
    
    def run_category_pipeline(self):
        """Run category search pipeline in a thread - runs forever"""
        while self.running:
            try:
                print(f"\n[CATEGORY PIPELINE] Starting...")
                self.category_pipeline.run_continuous(
                    delay_between_categories=1
                )
            except KeyboardInterrupt:
                print(f"\n[CATEGORY PIPELINE] Interrupted")
                break
            except Exception as e:
                print(f"\n[CATEGORY PIPELINE] Error: {e}")
                import traceback
                traceback.print_exc()
                if self.running:
                    print(f"[CATEGORY PIPELINE] Restarting in 10 seconds...")
                    time.sleep(10)
                    # Loop will continue and restart run_continuous
    
    def run_product_pipeline(self):
        """Run product extraction pipeline in a thread - runs forever"""
        while self.running:
            try:
                print(f"\n[PRODUCT PIPELINE] Starting...")
                self.product_pipeline.run_continuous(
                    delay_between_products=0.5
                )
            except KeyboardInterrupt:
                print(f"\n[PRODUCT PIPELINE] Interrupted")
                break
            except Exception as e:
                print(f"\n[PRODUCT PIPELINE] Error: {e}")
                import traceback
                traceback.print_exc()
                if self.running:
                    print(f"[PRODUCT PIPELINE] Restarting in 10 seconds...")
                    time.sleep(10)
                    # Loop will continue and restart run_continuous
    
    def run_health_check_server(self):
        """Run HTTP health check server for Railway with keep-alive - runs forever"""
        while self.running:
            try:
                port = int(os.getenv("PORT", "8080"))
                self.health_server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
                print(f"[*] Health check server started on port {port}")
                print(f"[*] Health endpoint: http://0.0.0.0:{port}/health")
                print(f"[*] Root endpoint: http://0.0.0.0:{port}/")
                
                # Set timeout to handle requests non-blockingly
                self.health_server.timeout = 1
                
                # Run server loop
                while self.running:
                    try:
                        self.health_server.handle_request()
                        time.sleep(0.1)  # Small delay between checks
                    except Exception as e:
                        print(f"[HEALTH SERVER] Request handling error: {e}")
                        time.sleep(1)
                
            except KeyboardInterrupt:
                print(f"\n[HEALTH SERVER] Interrupted")
                break
            except Exception as e:
                print(f"\n[HEALTH SERVER] Error: {e}")
                import traceback
                traceback.print_exc()
                if self.running:
                    print(f"[HEALTH SERVER] Restarting in 5 seconds...")
                    time.sleep(5)
                    # Loop will continue and restart server
    
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
            daemon=False
        )
        self.category_thread.start()
        print("[✓] Category Search Pipeline thread started")
        
        # Small delay before starting second pipeline
        time.sleep(2)
        
        # Start product extraction pipeline thread
        self.product_thread = threading.Thread(
            target=self.run_product_pipeline,
            name="ProductExtractionPipeline",
            daemon=False
        )
        self.product_thread.start()
        print("[✓] Product Extraction Pipeline thread started")
        
        # Start health check server thread (for Railway)
        self.health_check_thread = threading.Thread(
            target=self.run_health_check_server,
            name="HealthCheckServer",
            daemon=False
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
        
        # Keep-alive thread to prevent Railway from pausing
        def keep_alive_heartbeat():
            """Actively ping health endpoint and log to keep service alive"""
            import requests
            
            port = int(os.getenv("PORT", "8080"))
            health_url = f"http://localhost:{port}/health"
            
            while self.running:
                try:
                    # Ping health endpoint every 10 seconds to keep HTTP server active
                    time.sleep(10)
                    if self.running:
                        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                        
                        # Make actual HTTP request to keep service active
                        try:
                            response = requests.get(health_url, timeout=5)
                            print(f"\n[KEEP-ALIVE] {current_time} - Health check ping: {response.status_code}")
                        except Exception as e:
                            print(f"[KEEP-ALIVE] {current_time} - Health check ping failed: {e}")
                        
                        print(f"[KEEP-ALIVE] Service active, pipelines running...")
                        self.last_activity_time = time.time()
                        
                        # Check pipeline status
                        category_alive = self.category_thread and self.category_thread.is_alive()
                        product_alive = self.product_thread and self.product_thread.is_alive()
                        health_alive = self.health_check_thread and self.health_check_thread.is_alive()
                        
                        print(f"    Category Pipeline: {'✓ Running' if category_alive else '✗ Stopped'}")
                        print(f"    Product Pipeline: {'✓ Running' if product_alive else '✗ Stopped'}")
                        print(f"    Health Server: {'✓ Running' if health_alive else '✗ Stopped'}")
                        
                        # Restart pipelines if they died
                        if not category_alive and self.running:
                            print("[!] Category Pipeline stopped - restarting...")
                            self.category_thread = threading.Thread(
                                target=self.run_category_pipeline,
                                name="CategoryPipeline",
                                daemon=False
                            )
                            self.category_thread.start()
                        
                        if not product_alive and self.running:
                            print("[!] Product Pipeline stopped - restarting...")
                            self.product_thread = threading.Thread(
                                target=self.run_product_pipeline,
                                name="ProductExtractionPipeline",
                                daemon=False
                            )
                            self.product_thread.start()
                        
                        if not health_alive and self.running:
                            print("[!] Health Server stopped - restarting...")
                            self.health_check_thread = threading.Thread(
                                target=self.run_health_check_server,
                                name="HealthCheckServer",
                                daemon=False
                            )
                            self.health_check_thread.start()
                            
                except Exception as e:
                    print(f"[KEEP-ALIVE] Error: {e}")
                    import traceback
                    traceback.print_exc()
        
        keep_alive_thread = threading.Thread(
            target=keep_alive_heartbeat,
            name="KeepAlive",
            daemon=False
        )
        keep_alive_thread.start()
        print("[✓] Keep-alive heartbeat started\n")
    
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
        # Keep main thread alive indefinitely
        # This prevents the main process from exiting
        while True:
            time.sleep(1)
    except Exception as e:
        print(f"\n[✗] Fatal error: {e}")
        manager.stop()
        sys.exit(1)
    except KeyboardInterrupt:
        # This will be caught by signal_handler, but just in case
        manager.stop()
        sys.exit(0)


