def dpermutate(x, i=[1,2,3]):
  o = []
  for y in i:
    o.extend(permutations(x, y))
  return o

def permutations(x, l):
  o = []
  for i in range(0, len(x) - l + 1):
    o.append(x[i:i + l])
  return o
