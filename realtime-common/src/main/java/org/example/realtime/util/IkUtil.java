package org.example.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 中文分词工具类
 * @return 分词后的字符串列表
 */
public class IkUtil {
    public static List<String> IkSplit(String keywords){
        List<String> list = new ArrayList<>();

        StringReader stringReader = new StringReader(keywords);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                list.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return list;
    }
}
