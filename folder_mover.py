import os
import shutil
import time
import logging
import tarfile  # 引入 tarfile 库
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- 配置区 ---
SOURCE_DIR = "/home/hath/hath/download"
DEST_DIR = "/home/hath/downloaded_sync"
TRIGGER_FILE = "galleryinfo.txt"
LOG_FILE = "/home/hath/folder_mover.log"
# 新增配置：压缩完成后是否删除源文件夹
DELETE_SOURCE_AFTER_COMPRESSION = True 
# --- 配置结束 ---

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE),
                              logging.StreamHandler()])

def process_folder(sub_folder_path):
    """
    将处理文件夹的逻辑提取成一个独立的函数，方便复用。
    它负责压缩文件夹，并根据配置决定是否删除源文件夹。
    """
    sub_folder_name = os.path.basename(sub_folder_path)
    
    # 准备目标压缩文件的完整路径，例如 /home/hath/downloaded_sync/sub_folder_name.tar.gz
    archive_name = f"{sub_folder_name}.tar.gz"
    dest_archive_path = os.path.join(DEST_DIR, archive_name)

    logging.info(f"准备压缩文件夹 '{sub_folder_path}' 到 '{dest_archive_path}'")

    try:
        # 确保目标目录存在
        os.makedirs(DEST_DIR, exist_ok=True)
        
        # ---核心操作：创建 tar.gz 压缩文件---
        # 使用 'w:gz' 模式来创建 gzip 压缩的 tar 文件
        with tarfile.open(dest_archive_path, "w:gz") as tar:
            # arcname 参数指定了文件在压缩包内的相对路径，
            # 这里我们使用 sub_folder_name，这样解压后会得到一个文件夹，而不是一堆散乱的文件。
            tar.add(sub_folder_path, arcname=sub_folder_name)
        
        logging.info(f"成功创建压缩文件: '{archive_name}'")

        # --- (可选) 删除源文件夹 ---
        if DELETE_SOURCE_AFTER_COMPRESSION:
            logging.info(f"准备删除源文件夹: '{sub_folder_path}'")
            shutil.rmtree(sub_folder_path)
            logging.info(f"成功删除源文件夹: '{sub_folder_name}'")

    except Exception as e:
        logging.error(f"处理文件夹 '{sub_folder_name}' 时发生错误: {e}")
        # 如果压缩出错，最好不要删除源文件，所以删除操作放在 try 块的最后

class GalleryEventHandler(FileSystemEventHandler):
    """处理文件系统事件的处理器"""
    def on_created(self, event):
        self.handle_event(event)

    def on_moved(self, event):
        self.handle_event(event, path_to_check=event.dest_path)

    def handle_event(self, event, path_to_check=None):
        """核心事件处理逻辑"""
        if not path_to_check:
            path_to_check = event.src_path

        if os.path.basename(path_to_check) != TRIGGER_FILE:
            return
            
        sub_folder_path = os.path.dirname(path_to_check)
        
        if os.path.realpath(os.path.dirname(sub_folder_path)) != os.path.realpath(SOURCE_DIR):
            logging.warning(f"触发文件不在源目录的直接子文件夹中，忽略: {path_to_check}")
            return
            
        logging.info(f"检测到触发文件: {path_to_check}")
        
        # 调用独立的函数来处理文件夹
        process_folder(sub_folder_path)

def initial_scan():
    """脚本启动时，检查并处理已存在的合格文件夹"""
    logging.info("--- 启动时进行初始扫描 ---")
    if not os.path.exists(SOURCE_DIR):
        logging.warning(f"源目录 {SOURCE_DIR} 不存在，跳过初始扫描。")
        return

    # 使用 list(os.listdir(...)) 来创建一个静态列表，防止在循环中删除元素导致问题
    for item in list(os.listdir(SOURCE_DIR)):
        sub_folder_path = os.path.join(SOURCE_DIR, item)
        if os.path.isdir(sub_folder_path):
            trigger_file_path = os.path.join(sub_folder_path, TRIGGER_FILE)
            if os.path.exists(trigger_file_path):
                logging.info(f"在初始扫描中发现待处理文件夹: {sub_folder_path}")
                # 调用独立的函数来处理文件夹
                process_folder(sub_folder_path)
                
    logging.info("--- 初始扫描结束 ---")

if __name__ == "__main__":
    initial_scan()
    
    logging.info(f"开始监视目录: {SOURCE_DIR}")
    event_handler = GalleryEventHandler()
    observer = Observer()
    observer.schedule(event_handler, SOURCE_DIR, recursive=True)
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logging.info("监视器被手动停止。")
    observer.join()