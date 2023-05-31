from cerberus import Validator, errors
import yaml


class CustomErrorHandler(errors.BasicErrorHandler):
    messages = errors.BasicErrorHandler.messages.copy()
    messages[
        errors.REGEX_MISMATCH.code
    ] = "value does not match regex, must start with a number and end with m/h/d"


def validate_config_schema():
    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    schema_txt = """
    bucketing_interval:
        type: string
        required: true
        regex: ^[0-9]{1}[mhdMHD]$
    exchanges:
        type: list
        required: true
        schema:
            type: string
    list_coins_to_pull:
        nullable: true
        type: list
        required: true
        schema:
            type: string
    n_coins_to_pull:
        type: integer
        min: 1
        max: 100
        required: true
    database_connection:
        type: string
    save_to_csv:
        type: boolean
    save_to_database:
        type: boolean
    """

    schema = yaml.load(schema_txt, Loader=yaml.FullLoader)
    v = Validator(schema, error_handler=CustomErrorHandler)

    if not v.validate(config):
        raise ValueError(f"Error validating config. {v.errors}")
