import argparse
import os
import pathlib
import sys

# ----------------------------Setting root path------------------------------------
os.chdir(pathlib.Path(os.path.realpath("src")).parents[0])
# ## set path to sys for Lambda function
sys.path.insert(0, os.getcwd())

# ----------------------------Loading environment should be first to be loaded------------------
def __parseArguments():
    parser = argparse.ArgumentParser(description="load the component")

    parser.add_argument(
        "--action",
        choices=[
            "train",
            "predict",
        ],
        help="Which action or action pipeline to perform",
    )

    return parser.parse_args()


## Load arguments from the command line to be set in ENV variables
args = __parseArguments()

from src.services.ireland.pipelines.inference import predict_pipeline
from src.services.ireland.pipelines.training import train_pipeline

if __name__ == "__main__":
    if args.action == "train":
        train_pipeline.run()
    elif args.action == "predict":
        predict_pipeline.run()
    else:
        raise Exception("")
