import glob
from src.python.services.utils import DataHandler


def main():
    handler = DataHandler()

    path_refined = "src/resources/raw/*.csv"
    files_refined = glob.glob(path_refined)
    for file in files_refined:
        file_name = file.split("/")[3].split(".")[0]
        df = handler._extract_data(file)
        df = handler._transform_data(df, file_name)
        handler._load_data(df, file_name)


if __name__ == "__main__":
    main()
