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

image_extensions = ['.png', '.jpg', '.jpeg', '.webp', '.heic', '.heif', '.gif', '.tiff', '.tif']

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
            subprocess.run(['magick', str(filepath), '-compress', 'lossless', str(png_filepath)], check=True, stdout=subprocess.DEVNULL)
            filepath = png_filepath  # Use the PNG file for the rest of the process
            temp_png_created = True  # Set flag

        # Check image resolution using ffprobe
        ffprobe_process = subprocess.run(['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height', '-of', 'csv=s=x:p=0', str(filepath)], capture_output=True, text=True)
        width, height = map(int, ffprobe_process.stdout.strip().split('x'))
        resolution = width * height

        target_width = width
        target_height = height

        if resolution > max_resolution:
            # If the resolution is greater than max_resolution, scale it down
            target_width = round(width * (max_resolution / resolution) ** 0.5)
            target_height = round(height * (max_resolution / resolution) ** 0.5)

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

            cmd = ["ffmpeg", "-i", str(filepath), "-vf", f"scale={target_width}:{target_height}", "-c:v", "libsvtav1", "-usage", "allintra","-pix_fmt", pix_fmt, "-crf", str(quality), "-preset", "1", "-still-picture", "1", "-threads", "1", str(target_path), "-cpu-used", "0", "-y", "-hide_banner", "-loglevel", "error"]
        elif image_format == 'webp':
            cmd = ["ffmpeg", "-i", str(filepath), "-vf", f"scale={target_width}:{target_height}", "-c:v", "libwebp", "-lossless", "0", "-compression_level", "6", "-quality", str(quality), "-preset", preset, "-threads", "1", str(target_path), "-y", "-hide_banner", "-loglevel", "error"]

        # subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)
        assert cmd_runner(cmd)

        # Check if the image has DateTimeOriginal exif data using exiftool
        exiftool_process = subprocess.run(['exiftool', '-DateTimeOriginal', str(filepath), '-m'], capture_output=True, text=True)
        exif_output = exiftool_process.stdout

        if 'DateTimeOriginal' not in exif_output:
            # If there is no DateTimeOriginal data, set it to the file's modification time
            mod_time = datetime.fromtimestamp(filepath.stat().st_mtime)
            mod_time_str = mod_time.strftime('%Y:%m:%d %H:%M:%S')
            subprocess.run(['exiftool', '-DateTimeOriginal=' + mod_time_str, str(target_path), '-m'], check=True, stdout=subprocess.DEVNULL)

        # Copy all other exif data
        subprocess.run(['exiftool', '-TagsFromFile', str(filepath), '-all:all', str(target_path), '-m'], check=True, stdout=subprocess.DEVNULL)

        # Remove "_original" backup file created by exiftool
        backup_file = target_path.with_name(target_path.name + '_original')
        if backup_file.exists():
            backup_file.unlink()
        if temp_png_created:
            filepath.unlink()
    except Exception as e:
        error_message = traceback.format_exc()
        logging.error(f"Error processing image {filepath}: {e}\n{error_message}")


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
        files = [file for file in files if not ('@eaDir' in file or '@Recycle' in file)]
        non_imgfiles = [file for file in files if not file.lower().endswith(tuple(image_extensions))]
        if len(non_imgfiles) > 2:
            return False
        if len(files) > 0 and any(file.lower().endswith(tuple(image_extensions)) for file in files):
            return True
        return False

    for root, dirs, files in os.walk(source_dir):
        if is_imgfiles(files) and not '@' in root:
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

def process_comic_folder(comic_source_dir, source_dir, target_dir, quality, max_resolution, image_format, preset, color_depth):
    relative_comic_path = comic_source_dir.relative_to(source_dir)
    comic_target_dir = target_dir / relative_comic_path

    logging.info(f"Processing comic folder: {comic_source_dir}")

    # Create a temporary directory to store AVIF/WebP images
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Process each image in the comic folder
        modification_times = []
        for file in comic_source_dir.iterdir():
            if file.suffix.lower() in image_extensions:
                process_image(file, comic_source_dir, temp_dir_path, quality, max_resolution, image_format, preset, color_depth)
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
        # if galleryinfo.txt in comic_source_dir:
        if (comic_source_dir / 'galleryinfo.txt').exists():
            with open(comic_source_dir / 'galleryinfo.txt', 'r') as f:
                for line in f:
                    if line.startswith('Title:'):
                        galleryinfo['title'] = line.split(':', 1)[1].strip()
                    elif line.startswith('Author:'):
                        galleryinfo['author'] = line.split(':', 1)[1].strip()
                    elif line.startswith('Downloaded:'):
                        galleryinfo['download_time'] = datetime.strptime(line.split(':', 1)[1].strip(), '%Y-%m-%d %H:%M')
                    elif line.startswith('Tags:'):
                        galleryinfo['tags'] = line.split(':', 1)[1].strip()

        # Create ComicInfo.xml file
        create_comicinfo_xml_galleryinfo(temp_dir_path, galleryinfo)

        # Create CBZ file from the temporary directory
        cbz_filename = relative_comic_path.with_suffix('.cbz')
        cbz_path = target_dir / cbz_filename
        cbz_path.parent.mkdir(parents=True, exist_ok=True)
        compress_to_cbz(temp_dir_path, cbz_path)

        # Set CBZ file's creation and modification dates to match the source directory
        os.utime(cbz_path, (latest_mod_time.timestamp(), latest_mod_time.timestamp()))

    logging.info(f"Finished processing comic folder: {comic_source_dir}")


def submit_dir_comb(dir_comb, source_dir, target_dir, quality, max_resolution, image_format, preset, max_workers, color_depth):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        with tqdm(total=len(dir_comb), desc='Processing comic folders', unit='folder', ncols=80) as pbar:
            for (root, dirs, files) in dir_comb:
                comic_source_dir = Path(root)
                future = executor.submit(process_comic_folder, comic_source_dir, source_dir, target_dir, quality, max_resolution, image_format, preset, color_depth)
                futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"An error occurred during the conversion process: {e}")
                finally:
                    pbar.update(1)

def get_galleryinfo_dir_comb(source_dir, gallery_info):
    dir_comb = []

    def is_imgfiles(files):
        files = files.copy()
        files = [file for file in files if not '@' in file]
        files = [file for file in files if not file.lower().endswith('.txt')]
        non_imgfiles = [file for file in files if not file.lower().endswith(tuple(image_extensions))]
        if len(non_imgfiles) > 2:
            return False
        if len(files) > 0 and any(file.lower().endswith(tuple(image_extensions)) for file in files):
            return True
        return False

    for root, dirs, files in os.walk(source_dir):
        if is_imgfiles(files) and not '@' in root and (Path(root) / gallery_info).exists() and not 'finished' in root:
            dir_comb.append((root, dirs, files))
            print(root)
    return dir_comb

def main(input_dir, output_dir, quality, max_resolution, image_format, preset, max_workers, gallery_info, color_depth):
    source_dir = Path(input_dir)
    target_dir = Path(output_dir)
    finished_dir = source_dir / 'finished'

    os.environ["OMP_NUM_THREADS"] = "1"

    if gallery_info:
        finished_dir.mkdir(exist_ok=True)

        run_count = 0
        while True:
            dir_comb = get_galleryinfo_dir_comb(source_dir, gallery_info)
            if not dir_comb:
                time.sleep(60)
                continue

            submit_dir_comb(dir_comb, source_dir, target_dir, quality, max_resolution, image_format, preset, max_workers, color_depth)

            for root, _, _ in dir_comb:
                shutil.move(root, finished_dir / Path(root).name)
                
                if 'ehentai-daemon' in output_dir:
                    cbz_filename = Path(root).with_suffix('.cbz').name
                    shutil.move(target_dir / cbz_filename, Path("/mnt/synology/res/komga/240607-all-aio/") / cbz_filename)

            run_count += 1
            logging.info(f"Gallery info conversion run {run_count} completed.")


    else:
        dir_comb = get_img_dir_comb(source_dir)
    
        submit_dir_comb(dir_comb, source_dir, target_dir, quality, max_resolution, image_format, preset, max_workers, color_depth)


if __name__ == "__main__":
    import argparse

    setup_logging()
    logging.info("Starting comic folder conversion process...")

    parser = argparse.ArgumentParser(description="Convert comic folders to AVIF/WebP CBZ format.")
    parser.add_argument("input_dir", type=str, help="The input directory containing comic folders.")
    parser.add_argument("output_dir", type=str, help="The output directory to save the CBZ files.")
    parser.add_argument("--quality", type=int, default=35, help="CRF/quality value for AVIF/WebP conversion.")
    parser.add_argument("--max_resolution", type=int, default=3840*2160, help="Maximum resolution for images.")
    parser.add_argument("--format", type=str, choices=['avif', 'webp'], default='avif', help="Output image format: avif or webp.")
    parser.add_argument("--preset", type=str, choices=['default', 'picture', 'drawing', 'icon', 'text'], default='drawing', help="FFmpeg preset for WebP conversion.")
    parser.add_argument("--max_workers", type=int, default=multiprocessing.cpu_count(), help="Number of worker processes to use for parallel processing.")
    parser.add_argument("--gallery_info", type=str, help="Gallery info filename to trigger conversion process.")
    parser.add_argument("--color_depth", type=int, choices=[8, 10, 12], default=10, help="Color depth for AVIF conversion.")

    args = parser.parse_args()

    try:
        main(args.input_dir, args.output_dir, args.quality, args.max_resolution, args.format, args.preset, args.max_workers, args.gallery_info, args.color_depth)
        logging.info("Comic folder conversion process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the conversion process: {e}")