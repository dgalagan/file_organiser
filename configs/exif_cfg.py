exif_params = ["-j", "-G"]
include_tags = ["-all"]
exclude_tags = ["--File:Directory"]
EXIF_ARGS = [*exif_params, *include_tags, *exclude_tags]
BATCH_SIZE = 0
EXIF_CFG = {
    "args": EXIF_ARGS,
    "batch_size": BATCH_SIZE
}