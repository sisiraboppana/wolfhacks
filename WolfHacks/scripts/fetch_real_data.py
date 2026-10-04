"""Fetch an RR archive and ONE PPG-DaLiA subject from official public sources.

UCI does not support byte ranges. Stream ZIP local entries and stop after the selected
pickle, avoiding a mandatory download of every subject. No pickle is executed here.
"""
import argparse
import hashlib
import struct
import urllib.request
import zlib
from pathlib import Path
from zipfile import ZipFile

HRV_URL = 'https://zenodo.org/records/8171266/files/HRV_anonymized_data.zip?download=1'
DALIA_URL = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00495/data.zip'

def read_exact(stream, count):
    parts = bytearray()
    while len(parts) < count:
        chunk = stream.read(min(count-len(parts),1024*1024))
        if not chunk:
            raise ValueError('Dataset download ended unexpectedly')
        parts.extend(chunk)
    return bytes(parts)

def fetch_subject(subject, output):
    target = f'/{subject}/{subject}.pkl'
    temporary = output.with_suffix('.pkl.partial')
    with urllib.request.urlopen(DALIA_URL,timeout=60) as response:
        while True:
            header = read_exact(response,30)
            if header[:4] != b'PK\x03\x04':
                raise ValueError('Subject not found before ZIP directory; download the full UCI archive manually')
            _,_,flags,method,_,_,crc,size,expanded,name_len,extra_len = struct.unpack('<I5H3I2H',header)
            name = read_exact(response,name_len).decode('utf-8')
            extra = read_exact(response,extra_len)
            if flags & 9:
                raise ValueError('Streaming ZIP with descriptors/encryption unsupported; use full UCI download')
            if size == 0xffffffff or expanded == 0xffffffff:
                pos = 0
                while pos+4 <= len(extra):
                    tag,length = struct.unpack_from('<HH',extra,pos)
                    data = extra[pos+4:pos+4+length]
                    if tag == 1:
                        offset = 0
                        if expanded == 0xffffffff:
                            expanded = struct.unpack_from('<Q',data,offset)[0]; offset += 8
                        if size == 0xffffffff:
                            size = struct.unpack_from('<Q',data,offset)[0]
                    pos += 4+length
            selected = ('/'+name).endswith(target)
            print(f'{"Extracting" if selected else "Passing"} {name} ({size/1e6:.1f} MB)',flush=True)
            decoder = zlib.decompressobj(-15) if method == 8 else None
            if selected and method not in (0,8):
                raise ValueError('Unsupported ZIP compression')
            remaining = size
            checksum = written = 0
            stream = temporary.open('wb') if selected else None
            try:
                while remaining:
                    block = read_exact(response,min(remaining,1024*1024)); remaining -= len(block)
                    if stream:
                        block = decoder.decompress(block) if decoder else block
                        stream.write(block); checksum = zlib.crc32(block,checksum); written += len(block)
                if stream and decoder:
                    block = decoder.flush()
                    stream.write(block); checksum = zlib.crc32(block,checksum); written += len(block)
                    if not decoder.eof:
                        raise ValueError('Incomplete deflate stream')
            finally:
                if stream:
                    stream.close()
            if selected:
                if checksum != crc or written != expanded:
                    raise ValueError('Subject CRC or length mismatch; partial file retained for diagnosis')
                temporary.replace(output)
                print(f'Verified subject file: {output.resolve()}',flush=True)
                return

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset',choices=['hrv_acc','ppg_dalia'],required=True)
    parser.add_argument('--subject',default='S10',choices=[f'S{i}' for i in range(1,16)])
    args = parser.parse_args()
    root = Path('data/raw')/args.dataset
    root.mkdir(parents=True,exist_ok=True)
    if args.dataset == 'hrv_acc':
        archive = root/'HRV_anonymized_data.zip'
        if not archive.exists():
            urllib.request.urlretrieve(HRV_URL,archive)
        if hashlib.md5(archive.read_bytes()).hexdigest() != 'ce8482279536e54061b74641d00de158':
            raise ValueError('Official HRV-ACC archive checksum mismatch')
        with ZipFile(archive) as zipped:
            name = next(n for n in zipped.namelist() if n.endswith('/control_2.csv'))
            destination = root/'control_2.csv'
            destination.write_bytes(zipped.read(name))
        print(f'Verified recording: {destination.resolve()}')
    else:
        destination = root/f'{args.subject}.pkl'
        if destination.exists():
            print(f'Already present: {destination.resolve()}')
        else:
            fetch_subject(args.subject,destination)

if __name__ == '__main__':
    main()
