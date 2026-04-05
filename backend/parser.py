import lxml.etree as ET
import pyarrow as pa
import pyarrow.parquet as pq
import os
from shared.defines import IGNOREDDIRECTORIES

def get_schema_keys(xml_path):
    keys = set()
    context = ET.iterparse(xml_path, events=('end',), tag='row')
    
    for event, elem in context:
        keys.update(elem.attrib.keys())
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
            
    return list(keys)

def parse(xml_path, parquet_path):    
    all_keys = get_schema_keys(xml_path)
    chunk_size = 100000 
    records = []
    writer = None
    
    context = ET.iterparse(xml_path, events=('end',), tag='row')
    
    for event, elem in context:
        row_data = dict(elem.attrib)
        complete_row = {key: row_data.get(key, None) for key in all_keys}
        records.append(complete_row)
        
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
            
        if len(records) >= chunk_size:
            table = pa.Table.from_pylist(records)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)
            records = []

    if records:
        table = pa.Table.from_pylist(records)
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, table.schema)
        writer.write_table(table)
        
    if writer:
        writer.close()
            
def startParsing(path = ""):
    if(path == ""):
        dirs = [f for f in os.listdir() if not os.path.isfile(os.path.join(f)) and not f in IGNOREDDIRECTORIES]
        for i, dir in enumerate(dirs):
            print(f"{i} {dir}")
            
        folder = input()
        try:
            folder = int(folder)
        except:
            return
        
        dir = dirs[folder]
    else:
        dir = path
        
    files = [f for f in os.listdir(dir) if f.lower().endswith('.xml')]
    
    if len(files) == 0:
        print("No XML files in directory")
        return

    for file_name in files:
        full_path = os.path.join(dir, file_name)
        base_name = os.path.splitext(file_name)[0]
        full_parquet_path = os.path.join(dir, base_name + ".parquet")
        
        parse(full_path, full_parquet_path)