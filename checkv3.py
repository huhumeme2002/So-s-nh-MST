import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd 
import queue
import threading
import requests
import time
import re
import unicodedata
from difflib import SequenceMatcher
import logging
import os

# Cấu hình logging
logging.basicConfig(level=logging.DEBUG, filename="debug.log", filemode="w",
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Cấu hình API và thread
API_URL_TEMPLATE = "https://api.vietqr.io/v2/business/{taxCode}"
MAX_RETRIES = 15
MAX_THREADS = 20  # Số luồng tối đa

# Cấu hình giao diện CustomTkinter
ctk.set_appearance_mode("Dark")  # Chế độ tối mặc định
ctk.set_default_color_theme("blue")  # Chủ đề màu xanh

# Biến toàn cục
progress_count = 0
total_count = 0
progress_lock = threading.Lock()
df_global = None
df_lock = threading.Lock()
error_queue = queue.Queue()
retry_status = {}
retry_status_lock = threading.Lock()
pause_event = threading.Event()
pause_event.set()  # Mặc định là chạy
result_list = []
result_lock = threading.Lock()
session = requests.Session()
current_company_queue = queue.Queue()  # Hàng đợi để lưu thông tin công ty đang xử lý

# Danh sách từ viết tắt và từ dừng
abbreviation_map = {
    r"\bcty\b": "cong ty", r"\bcp\b": "co phan", r"\btnhh\b": "trach nhiem huu han",
    r"\bcn\b": "chi nhanh", r"\bxd\b": "xay dung", r"\btm\b": "thuong mai",
    r"\bvl\b": "vat lieu", r"\bjesco\b": "jsc", r"\bpccc\b": "phong chay chua chay",
    r"\bdv\b": "dich vu", r"\bsx\b": "san xuat", r"\bvlxd\b": "vat lieu xay dung",
    r"\btmdv\b": "thuong mai dich vu", r"\bmtv\b": "mot thanh vien",
    r"\bvt\b": "van tai", r"\bttnt\b": "khong xac dinh", r"\btmxd\b": "thuong mai xay dung",
    r"\bxnk\b": "xuat nhap khau", r"\bdntn\b": "doanh nghiep tu nhan",
    r"\bsxtm\b": "san xuat thuong mai", r"\bunc\b": "universal network connection",
    r"\bhtx\b": "hop tac xa", r"\bdvth\b": "dich vu thuong mai",
    r"\btnhh mtv\b": "trach nhiem huu han mot thanh vien", r"\bcty cp\b": "cong ty co phan",
    r"\bcty tnhh\b": "cong ty trach nhiem huu han", r"\btm & dv\b": "thuong mai va dich vu",
    r"\bsx-tm\b": "san xuat - thuong mai", r"\btm dv\b": "thuong mai dich vu",
    r"\bsx tm dv\b": "san xuat thuong mai dich vu", r"\btm dv xd\b": "thuong mai dich vu xay dung",
    r"\bcty tnhh xd\b": "cong ty trach nhiem huu han xay dung", r"\bcty co phan\b": "cong ty co phan",
    r"\btm-dv-xd\b": "thuong mai - dich vu - xay dung", r"\btm-dv\b": "thuong mai - dich vu",
    r"\btm - dv\b": "thuong mai - dich vu", r"\btnhh mot thanh vien\b": "trach nhiem huu han mot thanh vien",
    r"\bxd tm\b": "xay dung thuong mai", r"\btm dv sx\b": "thuong mai dich vu san xuat",
    r"\btnhh sx tm dv\b": "trach nhiem huu han san xuat thuong mai dich vu",
    r"\bsx & dv\b": "san xuat va dich vu", r"\bsx-tm-dv\b": "san xuat - thuong mai - dich vu",
    r"\btm-sx-dv\b": "thuong mai - san xuat - dich vu", r"\bdv bv\b": "dich vu bao ve"
}
abbreviation_map = {re.compile(k): v for k, v in abbreviation_map.items()}

stop_words = set([
    "cong ty", "trach nhiem huu han", "tnhh", "co phan", "cp", "chi nhanh", "cn",
    "xay dung", "thuong mai", "vat lieu", "jsc", "phong chay chua chay",
    "dich vu", "san xuat", "vat lieu xay dung", "thuong mai dich vu",
    "mot thanh vien", "van tai", "khong xac dinh", "thuong mai xay dung",
    "xuat nhap khau", "doanh nghiep tu nhan", "san xuat thuong mai",
    "universal network connection", "hop tac xa", "dich vu thuong mai",
    "trach nhiem huu han mot thanh vien", "cong ty co phan",
    "cong ty trach nhiem huu han", "thuong mai va dich vu",
    "san xuat - thuong mai", "san xuat thuong mai dich vu",
    "thuong mai dich vu xay dung", "cong ty trach nhiem huu han xay dung",
    "thuong mai - dich vu - xay dung", "thuong mai - dich vu",
    "xay dung thuong mai", "thuong mai dich vu san xuat",
    "trach nhiem huu han san xuat thuong mai dich vu", "san xuat va dich vu",
    "san xuat - thuong mai - dich vu", "thuong mai - san xuat - dich vu",
    "dich vu bao ve"
])

# Hàm gọi API tra cứu thông tin (chỉ lấy tên công ty)
def api_lookup(tax_id):
    url = API_URL_TEMPLATE.replace("{taxCode}", tax_id)
    for attempt in range(MAX_RETRIES):
        with retry_status_lock:
            retry_status[tax_id] = attempt
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == "00":
                    with retry_status_lock:
                        if tax_id in retry_status:
                            del retry_status[tax_id]
                    data = result.get("data", {})
                    return data.get("name", "Không có thông tin")
        except Exception as e:
            logging.error(f"Lỗi khi gọi API cho tax_id {tax_id}: {e}")
        time.sleep(1.5)  # Thời gian chờ giữa các lần thử lại
    with retry_status_lock:
        if tax_id in retry_status:
            del retry_status[tax_id]
    return "Error: Retry exhausted"

# Hàm xử lý công việc trong thread (chỉ xử lý tên công ty)
def worker(task_queue):
    while True:
        pause_event.wait()  # Đợi nếu tool bị tạm dừng
        try:
            index, tax_id = task_queue.get(timeout=10)  # Lấy task từ hàng đợi
            current_company_queue.put(tax_id)  # Thêm mã số thuế vào hàng đợi hiện tại
        except queue.Empty:  # Thoát nếu không còn task
            break
        company_name = api_lookup(tax_id)  # Gọi API để xử lý task
        with result_lock:  # Đảm bảo thread-safe khi thêm kết quả
            result_list.append((index, company_name))
        with progress_lock:  # Cập nhật tiến độ ngay sau mỗi task
            global progress_count
            progress_count += 1
        if company_name == "Error: Retry exhausted":  # Ghi lỗi nếu có
            error_queue.put(f"Mã số thuế {tax_id}: {company_name}")
        task_queue.task_done()  # Đánh dấu task hoàn thành

# Loại bỏ từ chung trong tên công ty
def remove_common_words(text1, text2):
    words1 = set(text1.split())
    words2 = set(text2.split())
    common_words = words1.intersection(words2)
    return " ".join([word for word in text1.split() if word not in common_words]), \
           " ".join([word for word in text2.split() if word not in common_words])

# Chuẩn hóa văn bản
def normalize_text(text):
    if pd.isna(text):
        return ""
    text = unicodedata.normalize("NFD", str(text)).encode("ascii", "ignore").decode("utf-8").lower()
    for pattern, replacement in abbreviation_map.items():
        text = pattern.sub(replacement, text)
    words = text.split()
    filtered_words = [word for word in words if word not in stop_words]
    return re.sub(r"\s+", " ", " ".join(filtered_words)).strip()

# Kiểm tra độ tương đồng
def check_similarity(text1, text2):
    normalized1 = normalize_text(text1)
    normalized2 = normalize_text(text2)
    if not normalized1 or not normalized2:
        return 0.0
    filtered1, filtered2 = remove_common_words(normalized1, normalized2)
    return SequenceMatcher(None, filtered1, filtered2).ratio()

# Xử lý file Excel (chỉ xử lý tên công ty)
def process_excel(file_path, max_workers):
    global progress_count, total_count, df_global, result_list
    try:
        df = pd.read_excel(file_path, dtype={'TaxID': str})
        logging.debug(f"Đã đọc file Excel: {file_path}, shape: {df.shape}")
    except Exception as e:
        logging.error(f"Lỗi khi đọc file Excel: {e}")
        messagebox.showerror("Lỗi", f"Không thể đọc file Excel: {e}")
        return

    if 'TaxID' not in df.columns:
        logging.error("File Excel không chứa cột 'TaxID'")
        messagebox.showerror("Lỗi", "File Excel phải chứa cột 'TaxID'")
        return

    for col in ["CompanyName", "Kết quả"]:
        if col not in df.columns:
            df[col] = "" if col != "Kết quả" else "KHÁC NHAU" if col == "Kết quả" and "company" in df.columns else ""

    with df_lock:
        df_global = df

    tasks = [(index, str(row['TaxID']).strip()) for index, row in df.iterrows() if str(row['TaxID']).strip()]
    total_count = len(tasks)
    progress_count = 0
    result_list = []

    task_queue = queue.Queue()
    for task in tasks:
        task_queue.put(task)

    threads = [threading.Thread(target=worker, args=(task_queue,)) for _ in range(max_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Lấy giá trị ngưỡng từ giao diện
    try:
        threshold1 = float(threshold1_entry.get()) / 100  # Chuyển từ phần trăm sang tỷ lệ (0-1)
        threshold2 = float(threshold2_entry.get()) / 100
        if not (0 <= threshold1 <= 1 and 0 <= threshold2 <= 1):
            raise ValueError("Ngưỡng phải nằm trong khoảng 0-100")
    except ValueError as e:
        logging.error(f"Lỗi nhập ngưỡng: {e}")
        messagebox.showerror("Lỗi", "Vui lòng nhập ngưỡng là số từ 0 đến 100")
        return

    with df_lock:
        for index, company_name in result_list:
            df_global.at[index, "CompanyName"] = company_name
        logging.debug(f"Đã cập nhật df_global với {len(result_list)} dòng")

        # So sánh tên công ty với 3 ngưỡng
        if "company" in df_global.columns:
            df_global["Similarity Company"] = df_global.apply(
                lambda row: check_similarity(row["CompanyName"], row["company"]), axis=1
            )
            df_global["Kết quả Company Threshold 1"] = df_global["Similarity Company"].apply(
                lambda x: "GIỐNG NHAU" if x > threshold1 else "KHÁC NHAU"
            )
            df_global["Kết quả Company Threshold 2"] = df_global["Similarity Company"].apply(
                lambda x: "GIỐNG NHAU" if x > threshold2 else "KHÁC NHAU"
            )
            df_global["Kết quả"] = df_global["Similarity Company"].apply(
                lambda x: "GIỐNG NHAU" if x > 0.90 else "KHÁC NHAU"
            )
            df_global["Company Consistency"] = df_global.apply(
                lambda row: "Consistent" if row["Kết quả Company Threshold 1"] == row["Kết quả Company Threshold 2"] == row["Kết quả"] else "Inconsistent",
                axis=1
            )

    if not file_path.endswith('.xlsx'):
        file_path += '.xlsx'

    try:
        with df_lock:
            if df_global is None or df_global.empty:
                raise ValueError("Không có dữ liệu để lưu")
            logging.debug(f"Lưu file với shape: {df_global.shape} vào {file_path}")
            df_global.to_excel(file_path, index=False)
        messagebox.showinfo("Hoàn thành", "Xử lý xong và file Excel đã được cập nhật!")
    except Exception as e:
        logging.error(f"Lỗi khi lưu file: {e}")
        messagebox.showerror("Lỗi", f"Không thể ghi file Excel: {e}")

# Cập nhật giao diện
def update_progress():
    if total_count > 0:
        progress_bar.set(progress_count / total_count)
    lbl_progress.configure(text=f"Đã xử lý: {progress_count} / {total_count} mã số thuế")
    logging.debug(f"Progress: {progress_count}/{total_count}")
    app.after(500, update_progress)

def update_retry_status():
    with retry_status_lock:
        lbl_retry.configure(text=f"Đang retry: {len(retry_status)}")
    app.after(500, update_retry_status)

def update_errors():
    while not error_queue.empty():
        error_text.insert("end", error_queue.get() + "\n")
        error_text.see("end")
    app.after(1000, update_errors)

def update_current_company():
    while not current_company_queue.empty():
        tax_id = current_company_queue.get()
        lbl_current_company.configure(text=f"Đang xử lý: {tax_id}")
    app.after(100, update_current_company)

# Các hàm giao diện
def change_theme(new_theme):
    ctk.set_appearance_mode(new_theme)

def select_file():
    file_path = filedialog.askopenfilename(title="Chọn file Excel", filetypes=[("Excel files", "*.xlsx;*.xls")])
    if file_path:
        error_text.delete("1.0", "end")
        multi_threaded = multi_thread_var.get()
        try:
            thread_count = int(thread_entry.get()) if multi_threaded else 1
            if thread_count <= 0:
                raise ValueError("Số luồng phải lớn hơn 0")
            thread_count = min(thread_count, MAX_THREADS)
            logging.debug(f"Bắt đầu xử lý file: {file_path} với {thread_count} luồng")
            threading.Thread(target=process_excel, args=(file_path, thread_count), daemon=True).start()
        except ValueError as e:
            messagebox.showerror("Lỗi", str(e) if "Số luồng phải" in str(e) else "Số luồng phải là số nguyên")

def toggle_pause_resume():
    if pause_event.is_set():
        pause_event.clear()
        btn_toggle.configure(text="Tiếp tục")
    else:
        pause_event.set()
        btn_toggle.configure(text="Tạm dừng")

def export_file():
    with df_lock:
        if df_global is not None:
            save_path = filedialog.asksaveasfilename(title="Xuất file Excel", defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx;*.xls")])
            if save_path:
                try:
                    df_global.to_excel(save_path, index=False)
                    messagebox.showinfo("Thành công", "Xuất file thành công!")
                except Exception as e:
                    logging.error(f"Lỗi khi xuất file: {e}")
                    messagebox.showerror("Lỗi", f"Không thể xuất file: {e}")
        else:
            messagebox.showwarning("Cảnh báo", "Chưa có dữ liệu để xuất.")

# Giao diện chính
app = ctk.CTk()
app.title("MST Crawl & So sánh tên công ty - Khánh")
app.geometry("600x400")

frame_options = ctk.CTkFrame(app)
frame_options.pack(pady=10, padx=10, fill="x", expand=False)

theme_option = ctk.CTkOptionMenu(frame_options, values=["Dark", "Light", "System"], command=change_theme, font=("Arial", 12))
theme_option.set("Dark")
theme_option.grid(row=0, column=0, padx=5, pady=5, sticky="w")

multi_thread_var = tk.BooleanVar()
chk_multi = ctk.CTkCheckBox(frame_options, text="Chạy nhanh hơn", variable=multi_thread_var, font=("Arial", 12))
chk_multi.grid(row=1, column=0, padx=5, pady=5, sticky="w")

thread_label = ctk.CTkLabel(frame_options, text="Số Request:", font=("Arial", 12))
thread_label.grid(row=2, column=0, padx=5, pady=5, sticky="w")

thread_entry = ctk.CTkEntry(frame_options, font=("Arial", 12))
thread_entry.insert(0, "4")
thread_entry.grid(row=2, column=1, padx=5, pady=5, sticky="w")

threshold1_label = ctk.CTkLabel(frame_options, text="Ngưỡng 1 (%):", font=("Arial", 12))
threshold1_label.grid(row=3, column=0, padx=5, pady=5, sticky="w")

threshold1_entry = ctk.CTkEntry(frame_options, font=("Arial", 12))
threshold1_entry.insert(0, "75")
threshold1_entry.grid(row=3, column=1, padx=5, pady=5, sticky="w")

threshold2_label = ctk.CTkLabel(frame_options, text="Ngưỡng 2 (%):", font=("Arial", 12))
threshold2_label.grid(row=4, column=0, padx=5, pady=5, sticky="w")

threshold2_entry = ctk.CTkEntry(frame_options, font=("Arial", 12))
threshold2_entry.insert(0, "90")
threshold2_entry.grid(row=4, column=1, padx=5, pady=5, sticky="w")

btn_select = ctk.CTkButton(app, text="Chọn file Excel", command=select_file, font=("Arial", 14))
btn_select.pack(pady=5)

btn_toggle = ctk.CTkButton(app, text="Tạm dừng", command=toggle_pause_resume, font=("Arial", 14))
btn_toggle.pack(pady=5)

btn_export = ctk.CTkButton(app, text="Xuất file", command=export_file, font=("Arial", 14))
btn_export.pack(pady=5)

lbl_progress = ctk.CTkLabel(app, text="Đã xử lý: 0 / 0 mã số thuế", font=("Arial", 12))
lbl_progress.pack(pady=5)

progress_bar = ctk.CTkProgressBar(app, mode="determinate")
progress_bar.pack(pady=5)
progress_bar.set(0)

lbl_retry = ctk.CTkLabel(app, text="Đang retry: 0", font=("Arial", 12), text_color="red")
lbl_retry.pack(pady=5)

lbl_current_company = ctk.CTkLabel(app, text="Đang xử lý: ", font=("Arial", 12))
lbl_current_company.pack(pady=5)

error_frame = ctk.CTkFrame(app)
error_frame.pack(pady=10, padx=10, fill="both", expand=True)

error_text = ctk.CTkTextbox(error_frame, height=100, font=("Arial", 12))
error_text.pack(side="left", fill="both", expand=True)

scrollbar = ctk.CTkScrollbar(error_frame, command=error_text.yview)
scrollbar.pack(side="right", fill="y")
error_text.configure(yscrollcommand=scrollbar.set)

update_progress()
update_retry_status()
app.after(1000, update_errors)
app.after(100, update_current_company)

app.mainloop()