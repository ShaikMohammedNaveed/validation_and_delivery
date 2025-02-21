# This module has the following structure:

InputConnectorInterface provides a unified interface to develop an input data source and integrate it into the pipeline.

All you need to do is initialize your child from this interface with arguments you need and overried \_load_data() function. In the function you need to populate self.\_input_batch.items with input items you have loaded.
Each InputItem has a `content` field for the notebook as str and `metadata` of type `InputMetadata`. `InputMetadata` provides 2 fields: status and data which you can populate or skip as necessary.

For simple example, please see `src/input_connectors/local_files.py`

In the example we read the files from a list of file names and populate the InputBatch with them.
