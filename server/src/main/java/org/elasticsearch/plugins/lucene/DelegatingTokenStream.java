/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.lucene;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.ESTokenStream;

import java.io.IOException;

public class DelegatingTokenStream extends TokenStream {
    private final ESTokenStream tokenStream;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);

    public DelegatingTokenStream(ESTokenStream tokenStream) {
        this.tokenStream = tokenStream;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        // clearAttributes();
        AnalyzeToken currentToken = tokenStream.incrementToken();

        if (currentToken == null) {
            return false;
        }
        setState(currentToken);
        return true;
    }

    private void setState(AnalyzeToken currentToken) {
        posIncrAtt.setPositionIncrement(currentToken.position());
        offsetAtt.setOffset(currentToken.startOffset(), currentToken.endOffset());
        typeAtt.setType(currentToken.type());
        posLenAtt.setPositionLength(currentToken.positionLength());
        termAtt.copyBuffer(currentToken.term(), 0, currentToken.termLen());
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        setState(tokenStream.reset());
    }

    @Override
    public void end() throws IOException {
        super.end();
        setState(tokenStream.end());
    }

    @Override
    public void close() throws IOException {
        super.close();
        tokenStream.close();
    }
}
