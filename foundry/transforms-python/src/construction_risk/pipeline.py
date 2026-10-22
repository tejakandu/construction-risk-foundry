from transforms.api import Pipeline

from construction_risk import datasets

my_pipeline = Pipeline()
my_pipeline.discover_transforms(datasets)
