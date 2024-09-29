import json
import os
from typing import Any, Dict, List
from typing import Tuple


class FileMetadata:
    def __init__(self, file_path: str, file_length: int, file_data: bytes):
        self.file_path: str = file_path
        self.file_length: int = file_length
        self.file_data: bytes = file_data

    @staticmethod
    def from_file(file_path: str) -> "FileMetadata":
        # Verify file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")

        file_length: int = os.path.getsize(file_path)
        with open(file_path, "rb") as file:
            file_data: bytes = file.read()
        return FileMetadata(file_path, file_length, file_data)

    def get_file_name(self) -> str:
        return os.path.basename(self.file_path)

    def __repr__(self) -> str:
        return f"FileMetadata(file_path={self.file_path}, file_length={self.file_length})"


class BatchFile:
    """
    Represents a batch FITS file, which is a collection of files concatenated together
    This file consists of a JSON header and a concatenation of the included files

    The JSON header occupies the first line of the file and contains the following fields:
    - count (int): the number of files in the batch
    - offset (int): the length of the header, used to offset each file's start position
    - files (List[str]): the names of the included files, sorted by their order in the batch
    - index (Dict[str, Tuple[int, int]]): a mapping of file names to their (start positions, file lengths) in the batch file
    """

    def __init__(self, fits_metadata_list: List[FileMetadata]):
        self.fits_metadata_list: List[FileMetadata] = fits_metadata_list

    @staticmethod
    def from_file(batch_file_path: str) -> "BatchFile":
        # Verify file exists
        if not os.path.exists(batch_file_path):
            raise FileNotFoundError(f"File {batch_file_path} not found")

        with open(batch_file_path, "rb") as file:
            header_str: str = file.readline().decode()
            header: Dict[str, Any] = json.loads(header_str)

            # Read individual files
            fits_metadata_list: List[FileMetadata] = []
            for fits_file_name, (start_pos, file_length) in header["index"].items():
                file.seek(header["offset"] + start_pos)
                file_data: bytes = file.read(file_length)
                fits_metadata_list.append(FileMetadata(fits_file_name, file_length, file_data))

        return BatchFile(fits_metadata_list)

    def compress(self, output_directory: str, output_file_name: str) -> None:
        """ Compresses individual files into a single batch file """
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        header: str = self._generate_header()
        with open(os.path.join(output_directory, output_file_name), "wb") as file:
            file.write(header.encode())
            file.write("\n".encode())

            for fits_metadata in self.fits_metadata_list:
                with open(fits_metadata.file_path, "rb") as fits_file:
                    file.write(fits_file.read())
                    file.write("\n".encode())

    def decompress(self, output_directory: str) -> None:
        """ Decompresses the batch file into individual files """
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        for fits_metadata in self.fits_metadata_list:
            with open(os.path.join(output_directory, fits_metadata.get_file_name()), "wb") as file:
                file.write(fits_metadata.file_data)

    def _generate_header(self) -> str:
        index: Dict[str, Tuple[int, int]] = {}
        files: List[str] = []
        current_offset: int = 0
        for fits_metadata in self.fits_metadata_list:
            fits_metadata: FileMetadata
            index[fits_metadata.get_file_name()] = (current_offset, fits_metadata.file_length)
            files.append(fits_metadata.get_file_name())
            current_offset += fits_metadata.file_length + 1  # Add 1 for the newline character at the end of each FITS file

        header: Dict[str, Any] = {
            "count": len(files),
            "offset": 0,  # temporarily set this to 0 to facilitate the calculation of the offset
            "files": files,
            "index": index
        }

        # Calculate header offset
        initial_header_str: str = json.dumps(header, separators=(',', ':'))
        header_offset: int = len(initial_header_str) + 1  # Add 1 for the newline character at the end of the header
        header_offset += len(str(header_offset)) - 1  # Add the length of the header offset itself, minus 1 for the initial 0

        # Update header with the correct offset
        header["offset"] = header_offset
        return json.dumps(header, separators=(',', ':'))

    def __repr__(self) -> str:
        return f"BatchFile[{len(self.fits_metadata_list)}]"


if __name__ == "__main__":
    import filecmp

    intput_directory: str = "C:/One/UCI/Alberto/data/fits/b0"
    output_directory: str = "C:/One/UCI/Alberto/RadonExploration/notebooks/v3/temp"
    output_decompressed_directory: str = os.path.join(output_directory, "decompressed")
    compressed_file_name: str = "compressed.batch"
    batch_size: int = 512

    # Load files
    files: List[str] = [file for file in os.listdir(intput_directory) if os.path.isfile(os.path.join(intput_directory, file))]
    file_metadata_list: List[FileMetadata] = [FileMetadata.from_file(os.path.join(intput_directory, file_name)) for file_name in files[:batch_size]]
    print(f"Loaded {len(file_metadata_list)} files")

    # Create batch file
    batch_file: BatchFile = BatchFile(file_metadata_list)
    batch_file.compress(output_directory, compressed_file_name)
    print(f"Compressed {batch_file} to {output_directory}")

    # Load batch file
    loaded_batch_file: BatchFile = BatchFile.from_file(os.path.join(output_directory, compressed_file_name))
    loaded_batch_file.decompress(output_decompressed_directory)
    print(f"Decompressed {batch_file} to {output_decompressed_directory}")

    # Validate before and after
    print("Validating files...")
    mismatched_files: List[Tuple[str, str]] = []
    for i, (original_metadata, loaded_metadata) in enumerate(zip(batch_file.fits_metadata_list, loaded_batch_file.fits_metadata_list)):
        i: int
        original_metadata: FileMetadata
        loaded_metadata: FileMetadata
        original_file_path: str = original_metadata.file_path
        loaded_file_path: str = os.path.join(output_decompressed_directory, original_metadata.get_file_name())

        print(f"- Comparing file #{i + 1}: {original_file_path} vs {loaded_file_path}... ", end="")
        file_match: bool = filecmp.cmp(original_file_path, loaded_file_path, shallow=False)
        print("match" if file_match else "mismatch")
        if not file_match:
            mismatched_files.append((original_file_path, loaded_file_path))

    print(f"Validation complete, {len(mismatched_files)} mismatched file(s)")
    if mismatched_files:
        print("Mismatched files:")
        for original_file_path, loaded_file_path in mismatched_files:
            print(f"- {original_file_path} vs {loaded_file_path}")
