import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import arxiv
import requests
import threading
import time
import csv
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='arxiv_downloader.log'
)

class ArxivDownloaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("arXiv Paper Downloader Pro")

        # Constants
        self.MAX_RESULTS = 200
        self.MIN_DELAY = 1.0  # Minimum delay between requests
        self.MAX_RETRIES = 3
        self.MIN_PDF_SIZE = 1024  # 1KB minimum for valid PDF
        self.MAX_WORKERS = 3  # Concurrent downloads
        self.REQUEST_TIMEOUT = 30  # seconds
        self.USER_AGENT = "arXivDownloader/1.0"

        # Variables
        self.search_var = tk.StringVar()
        self.delay_var = tk.DoubleVar(value=3.0)
        self.download_folder = tk.StringVar(value=os.getcwd())
        self.status_var = tk.StringVar(value="Ready")
        self.font_size = 10
        self.cancel_flag = False
        self.last_request_time = 0

        # Data
        self.results = []
        self.setup_ui()

    def get_headers(self):
        """Return standard headers for requests"""
        return {
            'User-Agent': self.USER_AGENT,
            'Accept': 'application/pdf',
        }

    def respectful_delay(self):
        """Enforce a polite delay between requests"""
        min_delay = max(self.MIN_DELAY, float(self.delay_var.get()))
        max_delay = min_delay * 1.5
        elapsed = time.time() - self.last_request_time
        wait_time = max(0, random.uniform(min_delay, max_delay) - elapsed)
        if wait_time > 0:
            time.sleep(wait_time)
        self.last_request_time = time.time()

    def setup_ui(self):
        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill='both', expand=True)

        # Search Section
        search_frame = ttk.LabelFrame(frame, text="Search Parameters", padding=5)
        search_frame.grid(row=0, column=0, columnspan=3, sticky='ew', pady=5)

        ttk.Label(search_frame, text="Search Query:").grid(row=0, column=0, sticky='w')
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=50)
        search_entry.grid(row=0, column=1, sticky='ew', padx=5)
        
        ttk.Label(search_frame, text="Delay (sec):").grid(row=0, column=2, sticky='w')
        ttk.Entry(search_frame, textvariable=self.delay_var, width=5).grid(row=0, column=3, sticky='w', padx=5)
        ttk.Button(search_frame, text="Browse", command=self.choose_folder).grid(row=0, column=4, sticky='e')

        # Folder path display
        ttk.Label(frame, textvariable=self.download_folder, foreground='gray').grid(row=1, column=0, columnspan=3, sticky='w')

        # Control Buttons
        control_frame = ttk.Frame(frame)
        control_frame.grid(row=2, column=0, columnspan=3, sticky='ew', pady=5)
        
        ttk.Button(control_frame, text="A+", command=self.increase_font).pack(side='left', padx=2)
        ttk.Button(control_frame, text="A−", command=self.decrease_font).pack(side='left', padx=2)
        ttk.Button(control_frame, text="Search", command=self.search_arxiv).pack(side='left', padx=10)
        ttk.Button(control_frame, text="Select All", command=self.select_all).pack(side='left', padx=2)
        ttk.Button(control_frame, text="Select None", command=self.select_none).pack(side='left', padx=2)
        ttk.Button(control_frame, text="Save to CSV", command=self.save_to_csv).pack(side='right', padx=2)
        ttk.Button(control_frame, text="Cancel", command=self.cancel_operation).pack(side='right', padx=10)

        # Results Treeview
        self.tree = ttk.Treeview(frame, columns=('Select', 'Title', 'Authors', 'Year', 'Category', 'arXiv ID', 'Pages', 'Size (KB)'),
                                show='headings', height=20)
        
        columns = {
            'Select': {'width': 50, 'anchor': 'center'},
            'Title': {'width': 300},
            'Authors': {'width': 150},
            'Year': {'width': 50, 'anchor': 'center'},
            'Category': {'width': 100},
            'arXiv ID': {'width': 100},
            'Pages': {'width': 50, 'anchor': 'center'},
            'Size (KB)': {'width': 70, 'anchor': 'center'}
        }
        
        for col, params in columns.items():
            self.tree.heading(col, text=col, command=lambda c=col: self.sort_by_column(c, False))
            self.tree.column(col, **params)
            
        self.tree.grid(row=3, column=0, columnspan=3, sticky='nsew')
        self.tree.bind("<Button-1>", self.on_tree_click)

        # Scrollbar
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=3, column=3, sticky='ns')

        # Download Section
        download_frame = ttk.Frame(frame)
        download_frame.grid(row=4, column=0, columnspan=3, sticky='ew', pady=5)
        
        ttk.Button(download_frame, text="Download Selected", command=self.download_selected).pack(side='right')
        self.progress = ttk.Progressbar(download_frame, mode='determinate')
        self.progress.pack(side='left', expand=True, fill='x', padx=5)
        ttk.Label(frame, textvariable=self.status_var, foreground="blue").grid(row=5, column=0, columnspan=3, sticky='w')

        # Configure grid weights
        frame.rowconfigure(3, weight=1)
        frame.columnconfigure(1, weight=1)

        self.set_font()
        search_entry.focus()
        self.root.bind_all("<Control-a>", lambda e: self.select_all())
        self.root.bind_all("<Command-a>", lambda e: self.select_all())

    def choose_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.download_folder.set(folder)

    def set_font(self):
        style = ttk.Style()
        style.configure("Treeview", font=("TkDefaultFont", self.font_size))
        style.configure("Treeview.Heading", font=("TkDefaultFont", self.font_size, "bold"))

    def increase_font(self):
        self.font_size = min(16, self.font_size + 1)
        self.set_font()

    def decrease_font(self):
        self.font_size = max(8, self.font_size - 1)
        self.set_font()

    def on_tree_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region == "cell":
            column = self.tree.identify_column(event.x)
            if column == '#1':  # Select column
                row = self.tree.identify_row(event.y)
                current = self.tree.set(row, 'Select')
                self.tree.set(row, 'Select', '☑' if current == '☐' else '☐')

    def select_all(self, event=None):
        for item in self.tree.get_children():
            self.tree.set(item, 'Select', '☑')
        self.status_var.set(f"Selected all {len(self.tree.get_children())} papers.")

    def select_none(self):
        for item in self.tree.get_children():
            self.tree.set(item, 'Select', '☐')
        self.status_var.set("Cleared all selections.")

    def cancel_operation(self):
        self.cancel_flag = True
        self.status_var.set("Operation cancelled by user.")

    def sort_by_column(self, col, descending):
        data = [(self.tree.set(child, col), child) for child in self.tree.get_children('')]
        
        # Try numeric sort first
        try:
            data.sort(key=lambda t: float(t[0]) if t[0].replace('.', '', 1).isdigit() else 0,
                      reverse=descending)
        except:
            data.sort(key=lambda t: t[0].lower(), reverse=descending)
            
        for index, (_, item) in enumerate(data):
            self.tree.move(item, '', index)
        self.tree.heading(col, command=lambda: self.sort_by_column(col, not descending))

    def validate_pdf(self, filepath):
        """Check if file exists and is a valid PDF"""
        if not os.path.exists(filepath):
            return False
            
        try:
            if os.path.getsize(filepath) < self.MIN_PDF_SIZE:
                return False
                
            with open(filepath, 'rb') as f:
                return f.read(4) == b'%PDF'
        except:
            return False

    def search_arxiv(self):
        query = self.search_var.get().strip()
        if not query:
            messagebox.showwarning("Input Required", "Please enter a search query.")
            return
            
        self.cancel_flag = False
        self.tree.delete(*self.tree.get_children())
        self.results.clear()
        self.status_var.set("Searching arXiv...")
        self.progress['value'] = 0

        def do_search():
            try:
                client = arxiv.Client(
                    page_size=50,
                    delay_seconds=5,
                    num_retries=3,
                    user_agent=self.USER_AGENT
                )
                
                search = arxiv.Search(
                    query=query,
                    max_results=self.MAX_RESULTS,
                    sort_by=arxiv.SortCriterion.Relevance
                )
                
                results = list(client.results(search))
                
                if self.cancel_flag:
                    self.status_var.set("Search cancelled.")
                    return
                
                self.progress['maximum'] = len(results)
                
                for i, result in enumerate(results, 1):
                    if self.cancel_flag:
                        break
                        
                    title = result.title.strip().replace("\n", " ")
                    authors = ", ".join(a.name.split()[-1] for a in result.authors[:3])
                    if len(result.authors) > 3:
                        authors += ", et al."
                    year = result.published.year
                    arxiv_id = result.get_short_id()
                    primary_category = result.primary_category
                    
                    # Get PDF info
                    self.respectful_delay()
                    pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
                    size_kb = 0
                    try:
                        with requests.head(pdf_url, headers=self.get_headers(), timeout=self.REQUEST_TIMEOUT) as r:
                            size_kb = int(r.headers.get('Content-Length', 0)) // 1024
                    except Exception as e:
                        logging.warning(f"Failed to get PDF info for {arxiv_id}: {e}")
                    
                    est_pages = size_kb // 150 if size_kb > 0 else 0
                    
                    self.results.append(result)
                    self.tree.insert('', 'end', values=(
                        '☐', title, authors, year, primary_category, 
                        arxiv_id, est_pages, size_kb
                    ))
                    
                    self.progress['value'] = i
                    self.status_var.set(f"Found {i} papers...")
                    self.root.update_idletasks()
                
                if not self.cancel_flag:
                    self.status_var.set(f"Found {len(results)} papers. Ready to download.")
                
            except Exception as e:
                logging.error(f"Search error: {e}")
                self.status_var.set("Search failed.")
                messagebox.showerror("Search Error", str(e))
            finally:
                self.progress['value'] = 0

        threading.Thread(target=do_search, daemon=True).start()

    def download_selected(self):
        selected_items = [item for item in self.tree.get_children() 
                         if self.tree.set(item, 'Select') == '☑']
        if not selected_items:
            messagebox.showinfo("No Selection", "Please select at least one paper to download.")
            return

        folder = self.download_folder.get()
        os.makedirs(folder, exist_ok=True)
        
        self.cancel_flag = False
        self.status_var.set("Preparing downloads...")
        self.progress['value'] = 0
        self.progress['maximum'] = len(selected_items)

        def download_paper(index, item):
            if self.cancel_flag:
                return (False, "Cancelled")
                
            paper = self.results[index]
            arxiv_id = paper.get_short_id()
            title = paper.title[:100].replace('/', '_').replace('\\', '_')
            filename = f"{title} - {arxiv_id}.pdf"
            filepath = os.path.join(folder, filename)
            
            # Skip if file already exists and is valid
            if os.path.exists(filepath) and self.validate_pdf(filepath):
                return (True, f"Skipped (exists): {filename}")
                
            pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
            
            for attempt in range(self.MAX_RETRIES):
                try:
                    self.respectful_delay()
                    headers = self.get_headers()
                    
                    with requests.get(pdf_url, headers=headers, stream=True, timeout=self.REQUEST_TIMEOUT) as r:
                        if r.status_code == 429:
                            retry_after = int(r.headers.get('Retry-After', 60))
                            self.status_var.set(f"Rate limited. Waiting {retry_after} seconds...")
                            time.sleep(retry_after)
                            continue
                            
                        r.raise_for_status()
                        
                        if 'application/pdf' not in r.headers.get('Content-Type', ''):
                            return (False, f"Invalid content type for {arxiv_id}")
                            
                        content_length = int(r.headers.get('Content-Length', 0))
                        if content_length < self.MIN_PDF_SIZE:
                            return (False, f"File too small for {arxiv_id}")
                            
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                if self.cancel_flag:
                                    os.remove(filepath)
                                    return (False, "Cancelled")
                                if chunk:
                                    f.write(chunk)
                                    
                        if not self.validate_pdf(filepath):
                            os.remove(filepath)
                            raise IOError("Downloaded file is not a valid PDF")
                            
                        return (True, f"Downloaded: {filename}")
                        
                except Exception as e:
                    if attempt == self.MAX_RETRIES - 1:
                        logging.error(f"Failed to download {arxiv_id}: {e}")
                        return (False, f"Failed: {filename} ({str(e)})")
                    wait_time = (2 ** attempt) + random.random()
                    time.sleep(wait_time)
                    
            return (False, f"Max retries reached for {filename}")

        def download_thread():
            total = len(selected_items)
            success_count = 0
            
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures = []
                for idx, item in enumerate(selected_items):
                    if self.cancel_flag:
                        break
                        
                    index = self.tree.index(item)
                    futures.append(executor.submit(download_paper, index, item))
                    
                for future in as_completed(futures):
                    if self.cancel_flag:
                        break
                        
                    success, message = future.result()
                    if success:
                        success_count += 1
                        
                    self.progress['value'] += 1
                    self.status_var.set(f"Downloaded {self.progress['value']}/{total} - {message}")
                    self.root.update_idletasks()
            
            if not self.cancel_flag:
                self.status_var.set(f"Download complete. {success_count}/{total} succeeded.")
                messagebox.showinfo("Done", f"Downloaded {success_count} of {total} papers.")
            else:
                self.status_var.set(f"Download cancelled. {success_count}/{total} completed.")

        threading.Thread(target=download_thread, daemon=True).start()

    def save_to_csv(self):
        if not self.tree.get_children():
            messagebox.showinfo("Nothing to Save", "No data to save. Perform a search first.")
            return
            
        default_filename = f"arxiv_results_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
            initialfile=default_filename
        )
        
        if not file_path:
            return
            
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([col for col in self.tree["columns"]])
                for item in self.tree.get_children():
                    writer.writerow([self.tree.set(item, col) for col in self.tree["columns"]])
                    
            self.status_var.set(f"Table saved to {os.path.basename(file_path)}")
            messagebox.showinfo("Success", "Data successfully exported to CSV.")
        except Exception as e:
            logging.error(f"CSV export error: {e}")
            messagebox.showerror("Export Error", f"Failed to save CSV: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    root.geometry("1100x750")
    
    # Style configuration
    style = ttk.Style()
    style.theme_use('clam')
    style.configure('TButton', padding=5)
    style.configure('TEntry', padding=5)
    
    app = ArxivDownloaderApp(root)
    root.mainloop()
