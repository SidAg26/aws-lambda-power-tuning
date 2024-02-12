def lambda_handler(event, context):
    input_dict = event['payload']
    i_min = input_dict['min']
    i_max = input_dict['max']
    i_step = input_dict['stepSize']
    
    input_list = [i for i in range(i_min, i_max, i_step)]
    return input_list