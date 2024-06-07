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

def process_image(filepath, source_dir, target_dir, quality, max_resolution, image_format, preset):
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
            cmd = ["ffmpeg", "-i", str(filepath), "-vf", f"scale={target_width}:{target_height}", "-c:v", "libsvtav1", "-crf", str(quality), "-still-picture", "1", str(target_path), "-cpu-used", "0", "-y", "-hide_banner", "-loglevel", "error"]
        elif image_format == 'webp':
            cmd = ["ffmpeg", "-i", str(filepath), "-vf", f"scale={target_width}:{target_height}", "-c:v", "libwebp", "-lossless", "0", "-compression_level", "6", "-quality", str(quality), "-preset", preset, str(target_path), "-y", "-hide_banner", "-loglevel", "error"]

        subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)

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
        logging.error(f"Error processing image {filepath}: {e}")

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
        files = [file for file in files if not '@' in file]
        if 'galleryinfo.txt' in files:
            files.remove('galleryinfo.txt')
            if all(file.lower().endswith(tuple(image_extensions)) for file in files):
                return True
        return False

    for root, dirs, files in os.walk(source_dir):
        if not dirs and is_imgfiles(files):
            dir_comb.append((root, dirs, files))
    return dir_comb

def process_comic_folder(source_dir, target_dir, quality, max_resolution, image_format, preset):
    source_dir = Path(source_dir)
    target_dir = Path(target_dir)

    dir_comb = get_img_dir_comb(source_dir)
            
    for root, dirs, files in tqdm(dir_comb, desc='Processing comic folders', unit='folder',ncols=80):
        comic_source_dir = Path(root)
        relative_comic_path = comic_source_dir.relative_to(source_dir)
        comic_target_dir = target_dir / relative_comic_path

        logging.info(f"Processing comic folder: {comic_source_dir}")

        # Create a temporary directory to store AVIF/WebP images
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)

            # Process each image in the comic folder
            for file in files:
                file_path = comic_source_dir / file
                if file_path.suffix.lower() in image_extensions:
                    process_image(file_path, comic_source_dir, temp_dir_path, quality, max_resolution, image_format, preset)
                else:
                    # Copy non-image files as is
                    target_non_image_path = temp_dir_path / file_path.relative_to(comic_source_dir)
                    target_non_image_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(file_path), str(target_non_image_path))

            # Create CBZ file from the temporary directory
            cbz_filename = relative_comic_path.with_suffix('.cbz')
            cbz_path = target_dir / cbz_filename
            cbz_path.parent.mkdir(parents=True, exist_ok=True)
            compress_to_cbz(temp_dir_path, cbz_path)

            # Set CBZ file's creation and modification dates to match the source directory
            source_stat = comic_source_dir.stat()
            os.utime(cbz_path, (source_stat.st_atime, source_stat.st_mtime))

        logging.info(f"Finished processing comic folder: {comic_source_dir}")

if __name__ == "__main__":
    import argparse

    setup_logging()
    logging.info("Starting comic folder conversion process...")

    parser = argparse.ArgumentParser(description="Convert comic folders to AVIF/WebP CBZ format.")
    parser.add_argument("input_dir", type=str, help="The input directory containing comic folders.")
    parser.add_argument("output_dir", type=str, help="The output directory to save the CBZ files.")
    parser.add_argument("--quality", type=int, default=80, help="CRF/quality value for AVIF/WebP conversion.")
    parser.add_argument("--max_resolution", type=int, default=3840*2160, help="Maximum resolution for images.")
    parser.add_argument("--format", type=str, choices=['avif', 'webp'], default='webp', help="Output image format: avif or webp.")
    parser.add_argument("--preset", type=str, choices=['default', 'picture', 'drawing', 'icon', 'text'], default='drawing', help="FFmpeg preset for WebP conversion.")

    args = parser.parse_args()

    try:
        process_comic_folder(args.input_dir, args.output_dir, args.quality, args.max_resolution, args.format, args.preset)
        logging.info("Comic folder conversion process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the conversion process: {e}")