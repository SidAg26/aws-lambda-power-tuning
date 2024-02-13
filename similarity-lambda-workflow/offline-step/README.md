## Offline Profiling AWS Step Functions Workflow

#### Files
- `initialiser.py` - takes workflow execution input as `.yml` or `.json` and creates versions and aliases for target function to run the profiling 
- `preProcessInputList.py` - pre-processes the state input to extract and form list of all the valid inputs to test in profiling stage
- `executor.py` - executes the target function versions/aliases in __parallel__  for all valid inputs and returns the list of output
- `cleanup.py` - removes all the created versions/aliases and sets back the target function to original configuration

----
### ToDo List <br>
- [ ] include AWS S3 payload
- [ ] pre-process input for input-similarity logic
- [ ] implement similarity logic
- [ ] implement logging/database for response analysis
- [ ] implement analysis function to log valid configurations
- [ ] implement agent for monitoring, similarity check and regular model updates