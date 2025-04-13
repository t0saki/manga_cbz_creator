import requests
import os
import shutil
import subprocess
import tempfile
import zipfile
import logging
from pathlib import Path
from datetime import datetime
from PIL import Image
import re
from tqdm import tqdm
import concurrent.futures
import multiprocessing
import time
import traceback

image_extensions = ['.png', '.jpg', '.jpeg', '.webp',
                    '.heic', '.heif', '.gif', '.tiff', '.tif']


def setup_logging():
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    log_filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S.log')
    log_filepath = log_dir / log_filename

    logging.basicConfig(
        filename=log_filepath,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def scan_library_with_env():
    """从环境变量读取配置并触发扫描的完整实现"""
    
    # 读取环境变量（新增超时配置）
    env_vars = {
        'base_url': os.getenv('KOMGA_BASE_URL'),
        'library_id': os.getenv('KOMGA_LIBRARY_ID'),
        # 'session': os.getenv('KOMGA_SESSION'),
        # 'remember_me': os.getenv('KOMGA_REMEMBER_ME'),
        'api_key': os.getenv('KOMGA_API_KEY'),
    }

    # 验证必要参数
    if not all(env_vars.values()):
        logging.error("Missing required environment variables")
        raise ValueError("Missing required environment variables")

    try:
        # 构造请求组件
        url = f"{env_vars['base_url']}/api/v1/libraries/{env_vars['library_id']}/scan"
        headers = {
            # "Cookie": f"SESSION={env_vars['session']}; remember-me={env_vars['remember_me']}",
            "X-API-Key": env_vars['api_key'],
            "User-Agent": "KomgaConverter/1.0"
        }

        response = requests.post(url, headers=headers)

        # 根据API文档处理响应
        if response.status_code == 202:
            logging.info(f"{env_vars['library_id']} Scan task triggered")
            return {"status": "success"}
        else:
            logging.error(f"Failed to trigger scan task  | code: {response.status_code} | 响应内容: {response.text[:200]}")
            return {"status": "error", "code": response.status_code}

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {str(e)}")
        return {"status": "exception", "error": str(e)}
    except ValueError as e:
        logging.error(f"Trigger scan task error: {str(e)}")
        return {"status": "config_error"}


def get_comic_date(comic_source_dir):
    """Determine the date for a comic folder.

    Prioiritizes 'Downloaded:' date from galleryinfo.txt.
    Falls back to latest modification time of images.
    Further falls back to the folder's modification time.
    """
    galleryinfo_path = comic_source_dir / 'galleryinfo.txt'
    if galleryinfo_path.exists():
        try:
            with open(galleryinfo_path, 'r', encoding='utf-8') as f: # Added encoding
                for line in f:
                    if line.startswith('Downloaded:'):
                        # Handle potential format variations if necessary
                        date_str = line.split(':', 1)[1].strip()
                        try:
                             # Try standard format first
                            return datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                        except ValueError:
                             # Add other formats if needed, e.g., with seconds
                             try:
                                 return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                             except ValueError:
                                 logging.warning(f"Could not parse date string '{date_str}' from {galleryinfo_path}. Falling back.")
                                 # Fall through if format is unexpected
        except Exception as e:
            logging.warning(f"Could not read or parse date from {galleryinfo_path}: {e}. Falling back.")
            # Fall through to use modification time

    # Fallback 1: Calculate latest modification time from image files
    modification_times = []
    try:
        for file in comic_source_dir.iterdir():
            if file.is_file() and file.suffix.lower() in image_extensions: # Check if file
                try:
                    modification_times.append(file.stat().st_mtime)
                except OSError as e:
                    logging.warning(f"Could not stat file {file}: {e}")
    except OSError as e:
         logging.warning(f"Could not iterate directory {comic_source_dir} for mod times: {e}")


    if modification_times:
        return datetime.fromtimestamp(max(modification_times))
    else:
        # Fallback 2: Use the folder's modification time
        try:
            return datetime.fromtimestamp(comic_source_dir.stat().st_mtime)
        except OSError as e:
            logging.warning(f"Could not stat directory {comic_source_dir}: {e}. Using current time as last resort.")
            # Last resort fallback
            return datetime.now()


def process_image(filepath, source_dir, target_dir, quality, max_resolution, image_format, preset, color_depth):
    relative_path = filepath.relative_to(source_dir)
    target_path = target_dir / relative_path
    target_path = target_path.with_suffix(f'.{image_format}')

    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)

        temp_png_created = False  # Flag to track if a temp PNG file is created

        if filepath.suffix.lower() in ['.heic', '.heif']:
            # Convert HEIC or HEIF to PNG using ImageMagick
            png_filepath = filepath.with_suffix('.png')
            subprocess.run(['magick', str(filepath), '-compress', 'lossless',
                           str(png_filepath)], check=True, stdout=subprocess.DEVNULL)
            filepath = png_filepath  # Use the PNG file for the rest of the process
            temp_png_created = True  # Set flag

        # Check image resolution using ffprobe
        ffprobe_process = subprocess.run(['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries',
                                         'stream=width,height', '-of', 'csv=s=x:p=0', str(filepath)], capture_output=True, text=True)
        width, height = map(int, ffprobe_process.stdout.strip().split('x'))
        resolution = width * height

        target_width = width
        target_height = height

        if resolution > max_resolution:
            # If the resolution is greater than max_resolution, scale it down
            target_width = round(width * (max_resolution / resolution) ** 0.5)
            target_height = round(
                height * (max_resolution / resolution) ** 0.5)

        if target_width % 2 != 0:
            target_width += 1
        if target_height % 2 != 0:
            target_height += 1

        if image_format == 'avif':
            if color_depth == 8:
                pix_fmt = 'yuv420p'
            elif color_depth == 10:
                pix_fmt = 'yuv420p10le'
            elif color_depth == 12:
                pix_fmt = 'yuv420p12'

            cmd = ["ffmpeg", "-i", str(filepath), "-vf", f"scale={target_width}:{target_height}", "-c:v", "libsvtav1", "-usage", "allintra", "-pix_fmt", pix_fmt, "-crf", str(
                quality), "-preset", "1", "-still-picture", "1", "-threads", "1", str(target_path), "-cpu-used", "0", "-y", "-hide_banner", "-loglevel", "error"]
        elif image_format == 'webp':
            cmd = ["ffmpeg", "-i", str(filepath), "-vf", f"scale={target_width}:{target_height}", "-c:v", "libwebp", "-lossless", "0", "-compression_level", "6", "-quality", str(
                quality), "-preset", preset, "-threads", "1", str(target_path), "-y", "-hide_banner", "-loglevel", "error"]

        # subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)
        assert cmd_runner(cmd)

        # Check if the image has DateTimeOriginal exif data using exiftool
        exiftool_process = subprocess.run(
            ['exiftool', '-DateTimeOriginal', str(filepath), '-m'], capture_output=True, text=True)
        exif_output = exiftool_process.stdout

        if 'DateTimeOriginal' not in exif_output:
            # If there is no DateTimeOriginal data, set it to the file's modification time
            mod_time = datetime.fromtimestamp(filepath.stat().st_mtime)
            mod_time_str = mod_time.strftime('%Y:%m:%d %H:%M:%S')
            subprocess.run(['exiftool', '-DateTimeOriginal=' + mod_time_str,
                           str(target_path), '-m'], check=True, stdout=subprocess.DEVNULL)

        # Copy all other exif data
        subprocess.run(['exiftool', '-TagsFromFile', str(filepath), '-all:all',
                       str(target_path), '-m'], check=True, stdout=subprocess.DEVNULL)

        # Remove "_original" backup file created by exiftool
        backup_file = target_path.with_name(target_path.name + '_original')
        if backup_file.exists():
            backup_file.unlink()
        if temp_png_created:
            filepath.unlink()
    except Exception as e:
        error_message = traceback.format_exc()
        logging.error(
            f"Error processing image {filepath}: {e}\n{error_message}")


def cmd_runner(cmd):
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        result.check_returncode()  # 检查命令是否执行成功
        return result
    except subprocess.CalledProcessError:
        logging.error(f"Error running command {cmd}: {result.stderr}")
        return False
    except Exception as e:
        logging.error(f"Error running command {cmd}: {e}")
        return False


def compress_to_cbz(source_dir, cbz_path):
    with zipfile.ZipFile(cbz_path, 'w') as cbz:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = Path(root) / file
                cbz.write(file_path, file_path.relative_to(source_dir))


def get_img_dir_comb(source_dir):
    dir_comb = []

    def is_imgfiles(files):
        files = files.copy()
        files = [file for file in files if not (
            '@eaDir' in file or '@Recycle' in file)]
        non_imgfiles = [file for file in files if not file.lower().endswith(
            tuple(image_extensions))]
        if len(non_imgfiles) > 2:
            return False
        if len(files) > 0 and any(file.lower().endswith(tuple(image_extensions)) for file in files):
            return True
        return False

    for root, dirs, files in os.walk(source_dir):
        if is_imgfiles(files) and not ('@eaDir' in root or '@Recycle' in root):
            dir_comb.append((root, dirs, files))
            print(root)
    return dir_comb


def create_comicinfo_xml(comic_target_dir, mod_time, comic_title):

    comicinfo_path = comic_target_dir / "ComicInfo.xml"
    comicinfo_content = f"""<?xml version="1.0" encoding="utf-8"?>
<ComicInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="ComicInfo.xsd">
  <Title>{comic_title}</Title>
  <Writer>{author}</Writer>
  <Year>{mod_time.year}</Year>
  <Month>{mod_time.month}</Month>
  <Day>{mod_time.day}</Day>
</ComicInfo>
"""
    with comicinfo_path.open('w', encoding='utf-8') as file:
        file.write(comicinfo_content)


def create_comicinfo_xml_galleryinfo(comic_target_dir, galleryinfo):
    comicinfo_path = comic_target_dir / "ComicInfo.xml"
    comicinfo_content = f"""<?xml version="1.0" encoding="utf-8"?>
<ComicInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="ComicInfo.xsd">
    <Title>{galleryinfo['title']}</Title>
    <Writer>{galleryinfo['author']}</Writer>
    <Year>{galleryinfo['download_time'].year}</Year>
    <Month>{galleryinfo['download_time'].month}</Month>
    <Day>{galleryinfo['download_time'].day}</Day>
    <Tags>{galleryinfo['tags']}</Tags>
</ComicInfo>
"""
    with comicinfo_path.open('w', encoding='utf-8') as file:
        file.write(comicinfo_content)


def process_comic_folder(comic_source_dir, source_dir, target_dir, quality, max_resolution, image_format, preset, color_depth, organize_by_date):
    relative_comic_path = comic_source_dir.relative_to(source_dir)

    logging.info(f"Processing comic folder: {comic_source_dir}")

    # Create a temporary directory to store AVIF/WebP images
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Process each image in the comic folder
        modification_times = []
        for file in comic_source_dir.iterdir():
            if file.is_file() and file.suffix.lower() in image_extensions:
                process_image(file, comic_source_dir, temp_dir_path, quality,
                              max_resolution, image_format, preset, color_depth)
                modification_times.append(file.stat().st_mtime)

        # Determine the latest modification time
        if modification_times:
            latest_mod_time = datetime.fromtimestamp(max(modification_times))
        else:
            latest_mod_time = datetime.now()  # Fallback if no images are found

        def extract_author(comic_title):
            match = re.search(r'\[(.*?)\]', comic_title)
            if match:
                return match.group(1)
            return None

        galleryinfo = {
            'title': comic_source_dir.name,
            'author': extract_author(comic_source_dir.name),
            'download_time': latest_mod_time,
            'tags': ""
        }
        
        if (comic_source_dir / 'galleryinfo.txt').exists():
            try:
                with open(comic_source_dir / 'galleryinfo.txt', 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.startswith('Title:'):
                            galleryinfo['title'] = line.split(':', 1)[1].strip()
                        elif line.startswith('Author:'):
                            galleryinfo['author'] = line.split(':', 1)[1].strip()
                        elif line.startswith('Downloaded:'):
                            try:
                                galleryinfo['download_time'] = datetime.strptime(
                                    line.split(':', 1)[1].strip(), '%Y-%m-%d %H:%M')
                            except ValueError:
                                logging.warning(f"Could not parse date from galleryinfo.txt. Using file modification time.")
                        elif line.startswith('Tags:'):
                            galleryinfo['tags'] = line.split(':', 1)[1].strip()
            except Exception as e:
                logging.warning(f"Error reading galleryinfo.txt: {e}. Using defaults.")

        # Create ComicInfo.xml file
        create_comicinfo_xml_galleryinfo(temp_dir_path, galleryinfo)

        # --- CBZ path calculation ---
        cbz_base_filename = f"{relative_comic_path.name}.cbz"

        # Use organize_by_date if enabled, but keep using latest_mod_time for organization
        if organize_by_date:
            comic_date = get_comic_date(comic_source_dir)  # Use new function for directory structure only
            year = comic_date.strftime('%Y')
            month = comic_date.strftime('%m')
            cbz_path = target_dir / year / month / cbz_base_filename
        else:
            cbz_path = target_dir / cbz_base_filename

        cbz_path.parent.mkdir(parents=True, exist_ok=True)
        compress_to_cbz(temp_dir_path, cbz_path)

        # Set CBZ file's creation and modification dates to match the source directory
        # Keep using the original timestamp logic
        os.utime(cbz_path, (latest_mod_time.timestamp(), latest_mod_time.timestamp()))

    logging.info(f"Finished processing comic folder: {comic_source_dir}")


def submit_dir_comb(dir_comb, source_dir, target_dir, quality, max_resolution, image_format, preset, max_workers, color_depth, organize_by_date):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        with tqdm(total=len(dir_comb), desc='Processing comic folders', unit='folder', ncols=80) as pbar:
            for (root, dirs, files) in dir_comb:
                comic_source_dir = Path(root)
                future = executor.submit(process_comic_folder, comic_source_dir, source_dir,
                                         target_dir, quality, max_resolution, image_format, preset, color_depth, organize_by_date)
                futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(
                        f"An error occurred during the conversion process: {e}")
                finally:
                    pbar.update(1)


def get_galleryinfo_dir_comb(source_dir, gallery_info):
    dir_comb = []

    def is_imgfiles(files):
        files = files.copy()
        files = [file for file in files if not (
            '@eaDir' in file or '@Recycle' in file)]
        files = [file for file in files if not file.lower().endswith('.txt')]
        non_imgfiles = [file for file in files if not file.lower().endswith(
            tuple(image_extensions))]
        if len(non_imgfiles) > 2:
            return False
        if len(files) > 0 and any(file.lower().endswith(tuple(image_extensions)) for file in files):
            return True
        return False

    for root, dirs, files in os.walk(source_dir):
        # Skip the 'finished' directory and hidden/system folders explicitly
        if 'finished' in Path(root).parts or '@eaDir' in root or '@Recycle' in root:
            dirs[:] = [] # Don't descend into these directories
            continue

        if is_imgfiles(files) and (Path(root) / gallery_info).exists():
            dir_comb.append((root, dirs, files))
            # print(root) # Optional: uncomment for debugging
    # Log the number of directories found
    logging.info(f"Found {len(dir_comb)} directories with '{gallery_info}' to process.")
    return dir_comb


def main(input_dir, output_dir, quality, max_resolution, image_format, preset, max_workers, gallery_info, color_depth, organize_by_date):
    source_dir = Path(input_dir)
    target_dir = Path(output_dir)
    finished_dir = source_dir / 'finished'

    os.environ["OMP_NUM_THREADS"] = "1"

    if gallery_info:
        finished_dir.mkdir(exist_ok=True)

        run_count = 0
        last_modified = False
        scan_needed = False # Flag to trigger scan only once after a batch
        while True:
            dir_comb = get_galleryinfo_dir_comb(source_dir, gallery_info)
            if not dir_comb:
                if scan_needed:
                    try:
                        scan_library_with_env()
                    except Exception as e:
                         logging.error(f"Failed to trigger Komga scan: {e}")
                    scan_needed = False # Reset flag after attempting scan

                logging.info(f"No new folders with {gallery_info} found. Waiting...")
                time.sleep(60)
                continue

            # Process the found folders
            submit_dir_comb(dir_comb, source_dir, target_dir, quality,
                            max_resolution, image_format, preset, max_workers, color_depth, organize_by_date)

            processed_roots = [] # Keep track of successfully processed roots
            for root, _, _ in dir_comb:
                processed_roots.append(Path(root)) # Add path for later use

            # Move processed folders and CBZ files *after* the batch is done
            for root_path in processed_roots:
                # --- Move source folder ---
                try:
                    target_finished_path = finished_dir / root_path.name
                    # Handle potential name collisions in finished dir (though unlikely with unique folder names)
                    if target_finished_path.exists():
                        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                        target_finished_path = finished_dir / f"{root_path.name}_{timestamp}"
                        logging.warning(f"Target finished path {finished_dir / root_path.name} exists. Moving source to {target_finished_path}")
                    shutil.move(str(root_path), target_finished_path) # Use str() for compatibility potentially
                    logging.info(f"Moved source folder {root_path} to {target_finished_path}")
                except Exception as e:
                    logging.error(f"Error moving source folder {root_path} to finished: {e}")
                    continue # Skip CBZ moving if source move failed

                # --- Move CBZ file (if applicable) ---
                if 'ehentai-daemon' in output_dir: # Check if the specific output logic applies
                    try:
                        # Construct CBZ filename
                        cbz_filename = f"{root_path.name}.cbz"

                        # Construct source CBZ path (potentially with date structure)
                        if organize_by_date:
                            # For finding the source file, we need to use get_comic_date
                            comic_date = get_comic_date(target_finished_path)
                            year = comic_date.strftime('%Y')
                            month = comic_date.strftime('%m')
                            source_cbz_path = target_dir / year / month / cbz_filename
                            final_dest_dir = Path("/mnt/synology/res/komga/Doujinsh-Lib-Era/") / year / month
                        else:
                            source_cbz_path = target_dir / cbz_filename
                            final_dest_dir = Path("/mnt/synology/res/komga/Doujinsh-Lib-Era/")

                        final_cbz_path = final_dest_dir / cbz_filename

                        if source_cbz_path.exists():
                            final_dest_dir.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(source_cbz_path), final_cbz_path)
                            logging.info(f"Moved CBZ {source_cbz_path} to {final_cbz_path}")
                        else:
                            logging.warning(f"CBZ file {source_cbz_path} not found for moving.")

                    except Exception as e:
                        logging.error(f"Error moving CBZ for {root_path.name}: {e}")

            run_count += 1
            scan_needed = True # Set flag to scan after this batch
            logging.info(f"Gallery info conversion run {run_count} completed for {len(processed_roots)} folders.")
            # Removed time.sleep here, will sleep at the start of the loop if nothing is found

    else: # Handle case without --gallery_info
        dir_comb = get_img_dir_comb(source_dir)
        if not dir_comb:
            logging.info("No image folders found to process.")
            return # Exit if nothing to do

        submit_dir_comb(dir_comb, source_dir, target_dir, quality,
                        max_resolution, image_format, preset, max_workers, color_depth, organize_by_date) # Pass organize_by_date
        # Note: CBZ moving logic specific to 'ehentai-daemon' is only in the gallery_info loop


if __name__ == "__main__":
    import argparse

    setup_logging()
    logging.info("Starting comic folder conversion process...")

    parser = argparse.ArgumentParser(
        description="Convert comic folders to AVIF/WebP CBZ format.")
    parser.add_argument("input_dir", type=str,
                        help="The input directory containing comic folders.")
    parser.add_argument("output_dir", type=str,
                        help="The output directory to save the CBZ files.")
    parser.add_argument("--quality", type=int, default=35,
                        help="CRF/quality value for AVIF/WebP conversion.")
    parser.add_argument("--max_resolution", type=int,
                        default=3840*2160, help="Maximum resolution for images.")
    parser.add_argument("--format", type=str, choices=[
                        'avif', 'webp'], default='avif', help="Output image format: avif or webp.")
    parser.add_argument("--preset", type=str, choices=['default', 'picture', 'drawing',
                        'icon', 'text'], default='drawing', help="FFmpeg preset for WebP conversion.")
    parser.add_argument("--max_workers", type=int, default=multiprocessing.cpu_count(),
                        help="Number of worker processes to use for parallel processing.")
    parser.add_argument("--gallery_info", type=str,
                        help="Gallery info filename to trigger conversion process.")
    parser.add_argument("--color_depth", type=int,
                        choices=[8, 10, 12], default=10, help="Color depth for AVIF conversion.")
    parser.add_argument("--organize_by_date", action='store_true',
                        help="Organize output CBZ files into YEAR/MONTH subdirectories based on comic date.")


    args = parser.parse_args()

    try:
        main(args.input_dir, args.output_dir, args.quality, args.max_resolution,
             args.format, args.preset, args.max_workers, args.gallery_info, args.color_depth, args.organize_by_date)
        logging.info("Comic folder conversion process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the conversion process: {e}")
