import json
import gzip

filename = "meta_Sports_and_Outdoors{0}{1}"

def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield json.dumps(eval(l))

f = open(filename.format('.json'), 'w')

for l in parse(filename.format('.json.gz')):
  f.write(l + '\n')
