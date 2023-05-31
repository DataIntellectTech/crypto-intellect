import yaml


def convert_bucket_interval():
    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    conversion_dict = {"M": 1, "H": 60, "D": 1440}

    bucket_interval = config["bucketing_interval"]
    digit_str = ""
    multiplier = 0
    interval_str = "MHD"

    for chr in bucket_interval:
        if chr.isdigit():
            digit_str += chr
        if chr.isalpha():
            if chr.upper() not in interval_str:
                raise ValueError(
                    f"{chr} not supported. Type of interval must be either M (minutes), H (hours) or D (days)"
                )

            multiplier = conversion_dict[chr.upper()]
            break

    interval_in_mins = int(digit_str) * multiplier

    config["bucketing_interval"] = interval_in_mins

    with open("/src/config.yml", "w") as f:
        yaml.dump(config, f)
