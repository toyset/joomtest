package com.github.toyset.joomtest;

import java.io.File;
import java.nio.charset.Charset;

public class SortLines {

    public static void main(String[] args) throws Exception {
        
        File in = new File(args[0]);
        File out = new File(args[1]);
        Charset charset = Charset.forName(args[2]);
        
        FileUtil.sortLines(in, out, charset);
    }

}
