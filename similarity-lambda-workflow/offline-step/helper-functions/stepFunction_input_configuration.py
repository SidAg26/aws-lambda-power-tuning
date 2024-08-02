[i for i in range(128, 3009, 128)]
print(len([i for i in range(10, 1000, 100)]))
config = {
  "lambdaARN": "arn:aws:lambda:ap-southeast-2:030103857128:function:workbench-matmul",
  "num": 3,
  "sla": {
    "value": "10"
  },
  "powerValues": [
    128,
    256,
    384,
    512,
    640,
    768,
    896,
    1024,
    1152,
    1280,
    1408,
    1536,
    1664,
    1792,
    1920,
    2048,
    2176,
    2304,
    2432,
    2560,
    2688,
    2816,
    2944,
    3008
  ],
  "payload": {
    "min": 10,
    "max": 10000,
    "stepSize": 100
  }
}