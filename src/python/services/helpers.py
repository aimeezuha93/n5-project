import yaml
import os


def get_yaml(relative_path: str, file: str):
    directory = os.path.join(os.path.abspath(os.curdir), relative_path)
    file_path = os.path.join(directory, file)

    with open(file_path) as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print.info(exc)


def get_casting(df, config, file):
    columns_rename = config[file]["columns_rename"]
    columns_text = config[file]["columns_text"]
    columns_integer = config[file]["columns_integer"]
    df.rename(columns=columns_rename, inplace=True)
    df.fillna(0)
    df = df.astype(columns_text)
    df = df.astype(columns_integer)

    return df
