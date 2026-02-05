import time

filename = "batch_001.jsonl"

def buf_count_newlines(fname):
    lines = 0
    buf_size = 1024 * 1024
    with open(fname, 'rb') as f:
        read_f = f.read
        buf = read_f(buf_size)
        while buf:
            lines += buf.count(b'\n')
            buf = read_f(buf_size)
    return lines

t0 = time.time()
count = buf_count_newlines(filename)
print(f"Lines: {count}")
print(f"Time: {time.time()-t0:.2f}s")