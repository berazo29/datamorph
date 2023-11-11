package com.example.datamorph.models;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
public class FileInformation {
    final private String filename;
    final private String schemaName;
}
