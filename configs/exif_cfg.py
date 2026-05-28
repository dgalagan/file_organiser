include_tags = ["-all"]
exclude_tags = ["--File:Directory"]
exif_params = ["-j", "-G"]
EXIF_CFG = [*exif_params, *include_tags, *exclude_tags]
BATCH_SIZE = 100