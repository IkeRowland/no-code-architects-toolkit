import os
import ffmpeg
import logging
import requests
import subprocess
import hashlib
import json
import re
import threading
import multiprocessing
from datetime import datetime, timedelta
from services.file_management import download_file
from services.gcp_toolkit import upload_to_gcs, GCP_BUCKET_NAME

# Set the default local storage directory
STORAGE_PATH = "/tmp/"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the path to the fonts directory
FONTS_DIR = '/usr/share/fonts/custom'

# Create the FONT_PATHS dictionary by reading the fonts directory
FONT_PATHS = {}
for font_file in os.listdir(FONTS_DIR):
    if font_file.endswith('.ttf') or font_file.endswith('.TTF'):
        font_name = os.path.splitext(font_file)[0]
        FONT_PATHS[font_name] = os.path.join(FONTS_DIR, font_file)

# Create a list of acceptable font names
ACCEPTABLE_FONTS = list(FONT_PATHS.keys())

class ProgressTracker:
    def __init__(self, total_frames):
        self.total_frames = total_frames
        self.current_frame = 0
        self.lock = threading.Lock()

    def update(self, frame):
        with self.lock:
            self.current_frame = frame

    def get_progress(self):
        with self.lock:
            return (self.current_frame / self.total_frames) * 100

def match_fonts():
    try:
        result = subprocess.run(['fc-list', ':family'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            fontconfig_fonts = result.stdout.split('\n')
            fontconfig_fonts = list(set(fontconfig_fonts))  # Remove duplicates
            matched_fonts = {}
            for font_file in FONT_PATHS.keys():
                for fontconfig_font in fontconfig_fonts:
                    if font_file.lower() in fontconfig_font.lower():
                        matched_fonts[font_file] = fontconfig_font.strip()

            # Parse and output the matched font names
            unique_font_names = set()
            for font in matched_fonts.values():
                font_name = font.split(':')[1].strip()
                unique_font_names.add(font_name)
            
            # Remove duplicates from font_name and sort them alphabetically
            unique_font_names = sorted(list(set(unique_font_names)))
            
            for font_name in unique_font_names:
                print(font_name)
        else:
            logger.error(f"Error matching fonts: {result.stderr}")
    except Exception as e:
        logger.error(f"Exception while matching fonts: {str(e)}")

match_fonts()

def generate_style_line(options):
    """Generate ASS style line from options."""
    style_options = {
        'Name': 'Default',
        'Fontname': options.get('font_name', 'Arial'),
        'Fontsize': options.get('font_size', 24),
        'PrimaryColour': options.get('primary_color', '&H00FFFFFF'),
        'OutlineColour': options.get('outline_color', '&H00000000'),
        'BackColour': options.get('back_color', '&H00000000'),
        'Bold': options.get('bold', 0),
        'Italic': options.get('italic', 0),
        'Underline': options.get('underline', 0),
        'StrikeOut': options.get('strikeout', 0),
        'ScaleX': 100,
        'ScaleY': 100,
        'Spacing': 0,
        'Angle': 0,
        'BorderStyle': 1,
        'Outline': options.get('outline', 1),
        'Shadow': options.get('shadow', 0),
        'Alignment': options.get('alignment', 2),
        'MarginL': options.get('margin_l', 10),
        'MarginR': options.get('margin_r', 10),
        'MarginV': options.get('margin_v', 10),
        'Encoding': options.get('encoding', 1),
        'OneWordHighlight': options.get('one_word_highlight', False)
    }
    return f"Style: {','.join(str(v) for v in style_options.values())}"

def process_subtitle_content(content, one_word_highlight):
    if not one_word_highlight:
        return content

    lines = content.split('\n')
    processed_lines = []
    for line in lines:
        if line.strip() and '-->' in line:
            # This is a subtitle line
            time_part, text_part = line.split('-->')
            start_time = datetime.strptime(time_part.strip(), '%H:%M:%S,%f')
            end_time = datetime.strptime(text_part.split(',')[0].strip(), '%H:%M:%S,%f')
            text = ','.join(text_part.split(',')[1:]).strip()
            
            words = re.findall(r'\S+', text)
            total_duration = (end_time - start_time).total_seconds()
            word_duration = total_duration / len(words)
            
            for i, word in enumerate(words):
                word_start = start_time + timedelta(seconds=i*word_duration)
                word_end = word_start + timedelta(seconds=word_duration)
                processed_lines.append(f"{word_start.strftime('%H:%M:%S,%f')[:-3]} --> {word_end.strftime('%H:%M:%S,%f')[:-3]}")
                processed_lines.append(f"{{\\highlight}}{{\\k{int(word_duration*100)}}}{word}")
                processed_lines.append('')
        else:
            processed_lines.append(line)
    
    return '\n'.join(processed_lines)

def validate_options(options):
    required_options = ['font_name', 'font_size', 'primary_color']
    for option in required_options:
        if option not in options:
            raise ValueError(f"Missing required option: {option}")
    
    if options.get('font_name') not in ACCEPTABLE_FONTS:
        raise ValueError(f"Invalid font name. Acceptable fonts are: {', '.join(ACCEPTABLE_FONTS)}")
    
    if not isinstance(options.get('font_size'), int) or options['font_size'] <= 0:
        raise ValueError("Font size must be a positive integer")

def get_job_hash(file_url, caption_srt, caption_type, options):
    job_data = json.dumps({
        'file_url': file_url,
        'caption_srt': caption_srt,
        'caption_type': caption_type,
        'options': options
    }, sort_keys=True).encode('utf-8')
    return hashlib.md5(job_data).hexdigest()

def process_captioning(file_url, caption_srt, caption_type, options, job_id):
    """Process video captioning using FFmpeg."""
    try:
        options = convert_array_to_collection(options)
        validate_options(options)

        job_hash = get_job_hash(file_url, caption_srt, caption_type, options)
        cache_path = os.path.join(STORAGE_PATH, f"{job_hash}_cache.json")
        
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as cache_file:
                cached_result = json.load(cache_file)
            logger.info(f"Job {job_id}: Retrieved result from cache")
            return cached_result['output_filename']

        logger.info(f"Job {job_id}: Starting download of file from {file_url}")
        video_path = download_file(file_url, STORAGE_PATH)
        logger.info(f"Job {job_id}: File downloaded to {video_path}")

        subtitle_extension = '.' + caption_type
        srt_path = os.path.join(STORAGE_PATH, f"{job_id}{subtitle_extension}")
        caption_style = ""
        one_word_highlight = options.get('one_word_highlight', False)

        if caption_type == 'ass':
            style_string = generate_style_line(options)
            caption_style = f"""
[Script Info]
Title: Highlight Current Word
ScriptType: v4.00+
[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
{style_string}
[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
            logger.info(f"Job {job_id}: Generated ASS style string: {style_string}")

        if caption_srt.startswith("https"):
            logger.info(f"Job {job_id}: Downloading caption file from {caption_srt}")
            response = requests.get(caption_srt)
            response.raise_for_status()
            if caption_type in ['srt','vtt']:
                with open(srt_path, 'wb') as srt_file:
                    srt_file.write(response.content)
            else:
                subtitle_content = caption_style + response.text
                subtitle_content = process_subtitle_content(subtitle_content, one_word_highlight)
                with open(srt_path, 'w') as srt_file:
                    srt_file.write(subtitle_content)
            logger.info(f"Job {job_id}: Caption file downloaded to {srt_path}")
        else:
            subtitle_content = caption_style + caption_srt
            subtitle_content = process_subtitle_content(subtitle_content, one_word_highlight)
            with open(srt_path, 'w') as srt_file:
                srt_file.write(subtitle_content)
            logger.info(f"Job {job_id}: SRT file created at {srt_path}")

        output_path = os.path.join(STORAGE_PATH, f"{job_id}_captioned.mp4")
        logger.info(f"Job {job_id}: Output path set to {output_path}")

        font_name = options.get('font_name', 'Arial')
        if font_name in FONT_PATHS:
            selected_font = FONT_PATHS[font_name]
            logger.info(f"Job {job_id}: Font path set to {selected_font}")
        else:
            selected_font = FONT_PATHS.get('Arial')
            logger.warning(f"Job {job_id}: Font {font_name} not found. Using default font Arial.")

        if subtitle_extension == '.ass':
            if one_word_highlight:
                subtitle_filter = f"ass='{srt_path}',subtitles='{srt_path}':force_style='Highlight=1'"
            else:
                subtitle_filter = f"subtitles='{srt_path}'"
            logger.info(f"Job {job_id}: Using ASS subtitle filter: {subtitle_filter}")
        else:
            subtitle_filter = f"subtitles={srt_path}:force_style='"
            style_options = {
                'FontName': font_name,
                'FontSize': options.get('font_size', 24),
                'PrimaryColour': options.get('primary_color', '&H00FFFFFF'),
                'SecondaryColour': options.get('secondary_color', '&H00000000'),
                'OutlineColour': options.get('outline_color', '&H00000000'),
                'BackColour': options.get('back_color', '&H00000000'),
                'Bold': options.get('bold', 0),
                'Italic': options.get('italic', 0),
                'Underline': options.get('underline', 0),
                'StrikeOut': options.get('strikeout', 0),
                'Alignment': options.get('alignment', 2),
                'MarginV': options.get('margin_v', 10),
                'MarginL': options.get('margin_l', 10),
                'MarginR': options.get('margin_r', 10),
                'Outline': options.get('outline', 1),
                'Shadow': options.get('shadow', 0),
                'Blur': options.get('blur', 0),
                'BorderStyle': options.get('border_style', 1),
                'Encoding': options.get('encoding', 1),
                'Spacing': options.get('spacing', 0),
                'Angle': options.get('angle', 0),
                'UpperCase': options.get('uppercase', 0),
                'Highlight': 1 if one_word_highlight else None
            }

            subtitle_filter += ','.join(f"{k}={v}" for k, v in style_options.items() if v is not None)
            subtitle_filter += "'"
            logger.info(f"Job {job_id}: Using subtitle filter: {subtitle_filter}")

        probe = ffmpeg.probe(video_path)
        total_frames = int(probe['streams'][0]['nb_frames'])
        progress = ProgressTracker(total_frames)

        def on_progress(frame):
            progress.update(frame)
            logger.info(f"Job {job_id}: Progress {progress.get_progress():.2f}%")

        try:
            logger.info(f"Job {job_id}: Running FFmpeg with filter: {subtitle_filter}")
            ffmpeg.input(video_path).output(
                output_path,
                vf=subtitle_filter,
                acodec='copy'
            ).run(capture_stdout=True, capture_stderr=True, overwrite_output=True, progress=on_progress)
            logger.info(f"Job {job_id}: FFmpeg processing completed, output file at {output_path}")
        except ffmpeg.Error as e:
            if e.stderr:
                error_message = e.stderr.decode('utf8')
            else:
                error_message = 'Unknown FFmpeg error'
            logger.error(f"Job {job_id}: FFmpeg error: {error_message}")
            raise

        output_filename = upload_to_gcs(output_path, GCP_BUCKET_NAME)
        logger.info(f"Job {job_id}: File uploaded to GCS at {output_filename}")

        os.remove(video_path)
        os.remove(srt_path)
        os.remove(output_path)
        logger.info(f"Job {job_id}: Local files cleaned up")

        with open(cache_path, 'w') as cache_file:
            json.dump({'output_filename': output_filename}, cache_file)

        return output_filename
    except requests.RequestException as re:
        logger.error(f"Job {job_id}: Error downloading file: {str(re)}")
        raise
    except ValueError as ve:
        logger.error(f"Job {job_id}: Validation error: {str(ve)}")
        raise
    except ffmpeg.Error as fe:
        logger.error(f"Job {job_id}: FFmpeg error: {fe.stderr.decode('utf8') if fe.stderr else 'Unknown FFmpeg error'}")
        raise
    except Exception as e:
        logger.error(f"Job {job_id}: Unexpected error in process_captioning: {str(e)}")
        raise

def convert_array_to_collection(options):
    logger.info(f"Converting options array to dictionary: {options}")
    return {item["option"]: item["value"] for item in options}

def process_captioning_wrapper(args):
    return process_captioning(*args)

def batch_process_captioning(job_list):
    with multiprocessing.Pool() as pool:
        results = pool.map(process_captioning_wrapper, job_list)
    return results

# Usage example:
if __name__ == "__main__":
    # Single job processing
    file_url = "https://example.com/video.mp4"
    caption_srt = "https://example.com/captions.srt"
    caption_type = "srt"
    options = [
        {"option": "font_name", "value": "Arial"},
        {"option": "font_size", "value": 24},
        {"option": "primary_color", "value": "&H00FFFFFF"},
        {"option": "one_word_highlight", "value": True}
    ]
    job_id = "example_job_1"

    try:
        result = process_captioning(file_url, caption_srt, caption_type, options, job_id)
        print(f"Job {job_id} completed. Output file: {result}")
    except Exception as e:
        print(f"Error processing job {job_id}: {str(e)}")

    # Batch processing example
    job_list = [
        (file_url, caption_srt, caption_type, options, "batch_job_1"),
        (file_url, caption_srt, caption_type, options, "batch_job_2"),
        # Add more jobs as needed
    ]

    try:
        results = batch_process_captioning(job_list)
        for job_id, result in zip([job[4] for job in job_list], results):
            print(f"Batch job {job_id} completed. Output file: {result}")
    except Exception as e:
        print(f"Error in batch processing: {str(e)}")                    
