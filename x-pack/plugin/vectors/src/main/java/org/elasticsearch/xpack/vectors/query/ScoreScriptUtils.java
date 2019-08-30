/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder.sortSparseDimsFloatValues;

public class ScoreScriptUtils {

    //**************FUNCTIONS FOR DENSE VECTORS
    // Functions are implemented as classes to accept a hidden parameter scoreScript that contains some index settings.
    // Also, constructors for some functions accept queryVector to calculate and cache queryVectorMagnitude only once
    // per script execution for all documents.

    // Calculate l1 norm (Manhattan distance) between a query's dense vector and documents' dense vectors
    public static final class L1Norm {
        private final ScoreScript scoreScript;
        public L1Norm(ScoreScript scoreScript) {
            this.scoreScript = scoreScript;
        }

        public double l1norm(List<Number> queryVector, VectorScriptDocValues.DenseVectorScriptDocValues dvs){
            BytesRef vector = dvs.getEncodedValue();
            int vectorLength = VectorEncoderDecoder.denseVectorLength(scoreScript._getIndexVersion(), vector);
            if (queryVector.size() != vectorLength) {
                throw new IllegalArgumentException("Can't calculate l1norm! The number of dimensions of the query vector [" +
                    queryVector.size() + "] is different from the documents' vectors [" + vectorLength + "].");
            }

            Iterator<Number> queryVectorIter = queryVector.iterator();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double l1norm = 0;
            for (int dim = 0; dim < vectorLength; dim++) {
                double queryValue = queryVectorIter.next().floatValue();
                double docValue = byteBuffer.getFloat();
                l1norm += Math.abs(queryValue - docValue);
            }
            return l1norm;
        }
    }

    // Calculate l2 norm (Euclidean distance) between a query's dense vector and documents' dense vectors
    public static final class L2Norm {
        private final ScoreScript scoreScript;
        public L2Norm(ScoreScript scoreScript) {
            this.scoreScript = scoreScript;
        }

        public double l2norm(List<Number> queryVector, VectorScriptDocValues.DenseVectorScriptDocValues dvs){
            BytesRef vector = dvs.getEncodedValue();
            int vectorLength = VectorEncoderDecoder.denseVectorLength(scoreScript._getIndexVersion(), vector);
            if (queryVector.size() != vectorLength) {
                throw new IllegalArgumentException("Can't calculate l2norm! The number of dimensions of the query vector [" +
                    queryVector.size() + "] is different from the documents' vectors [" + vectorLength + "].");
            }

            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);
            Iterator<Number> queryVectorIter = queryVector.iterator();

            double l2norm = 0;
            for (int dim = 0; dim < vectorLength; dim++) {
                double diff = queryVectorIter.next().floatValue() - byteBuffer.getFloat();
                l2norm += diff * diff;
            }
            return Math.sqrt(l2norm);
        }
    }

    // Calculate a dot product between a query's dense vector and documents' dense vectors
    public static final class DotProduct {
        private final ScoreScript scoreScript;
        public DotProduct(ScoreScript scoreScript){
            this.scoreScript = scoreScript;
        }

        public double dotProduct(List<Number> queryVector, VectorScriptDocValues.DenseVectorScriptDocValues dvs){
            BytesRef vector = dvs.getEncodedValue();
            int vectorLength = VectorEncoderDecoder.denseVectorLength(scoreScript._getIndexVersion(), vector);
            if (queryVector.size() != vectorLength) {
                throw new IllegalArgumentException("Can't calculate dotPRoduct! The number of dimensions of the query vector [" +
                    queryVector.size() + "] is different from the documents' vectors [" + vectorLength + "].");
            }

            Iterator<Number> queryVectorIter = queryVector.iterator();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double dotProduct = 0;
            for (int dim = 0; dim < vectorLength; dim++) {
                dotProduct += queryVectorIter.next().floatValue() * byteBuffer.getFloat();
            }
            return dotProduct;
        }
    }

    // Calculate cosine similarity between a query's dense vector and documents' dense vectors
    public static final class CosineSimilarity {
        private final ScoreScript scoreScript;
        final double queryVectorMagnitude;
        final List<Number> queryVector;

        // calculate queryVectorMagnitude once per query execution
        public CosineSimilarity(ScoreScript scoreScript, List<Number> queryVector) {
            this.scoreScript = scoreScript;
            this.queryVector = queryVector;

            double dotProduct = 0;
            for (Number value : queryVector) {
                float floatValue = value.floatValue();
                dotProduct += floatValue * floatValue;
            }
            this.queryVectorMagnitude = Math.sqrt(dotProduct);
        }

        public double cosineSimilarity(VectorScriptDocValues.DenseVectorScriptDocValues dvs) {
            BytesRef vector = dvs.getEncodedValue();
            int vectorLength = VectorEncoderDecoder.denseVectorLength(scoreScript._getIndexVersion(), vector);
            if (queryVector.size() != vectorLength) {
                throw new IllegalArgumentException("Can't calculate cosineSimilarity! The number of dimensions of the query vector [" +
                    queryVector.size() + "] is different from the documents' vectors [" + vectorLength + "].");
            }

            Iterator<Number> queryVectorIter = queryVector.iterator();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double dotProduct = 0.0;
            double docVectorMagnitude = 0.0f;
            if (scoreScript._getIndexVersion().onOrAfter(Version.V_7_4_0)) {
                for (int dim = 0; dim < vectorLength; dim++) {
                    dotProduct += queryVectorIter.next().floatValue() * byteBuffer.getFloat();
                }
                docVectorMagnitude = VectorEncoderDecoder.decodeVectorMagnitude(scoreScript._getIndexVersion(), vector);
            } else {
                for (int dim = 0; dim < vectorLength; dim++) {
                    float docValue = byteBuffer.getFloat();
                    dotProduct += queryVectorIter.next().floatValue() * docValue;
                    docVectorMagnitude += docValue * docValue;
                }
                docVectorMagnitude = (float) Math.sqrt(docVectorMagnitude);
            }
            return dotProduct / (docVectorMagnitude * queryVectorMagnitude);
        }
    }

    //**************FUNCTIONS FOR SPARSE VECTORS
    // Functions are implemented as classes to accept a hidden parameter scoreScript that contains some index settings.
    // Also, constructors for some functions accept queryVector to calculate and cache queryVectorMagnitude only once
    // per script execution for all documents.

    public static class VectorSparseFunctions {
        final ScoreScript scoreScript;
        final float[] queryValues;
        final int[] queryDims;

        // prepare queryVector once per script execution
        // queryVector represents a map of dimensions to values
        public VectorSparseFunctions(ScoreScript scoreScript, Map<String, Number> queryVector) {
            this.scoreScript = scoreScript;
            //break vector into two arrays dims and values
            int n = queryVector.size();
            queryValues = new float[n];
            queryDims = new int[n];
            int i = 0;
            for (Map.Entry<String, Number> dimValue : queryVector.entrySet()) {
                try {
                    queryDims[i] = Integer.parseInt(dimValue.getKey());
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Failed to parse a query vector dimension, it must be an integer!", e);
                }
                queryValues[i] = dimValue.getValue().floatValue();
                i++;
            }
            // Sort dimensions in the ascending order and sort values in the same order as their corresponding dimensions
            sortSparseDimsFloatValues(queryDims, queryValues, n);
        }
    }

    // Calculate l1 norm (Manhattan distance) between a query's sparse vector and documents' sparse vectors
    public static final class L1NormSparse extends VectorSparseFunctions {
        public L1NormSparse(ScoreScript scoreScript,Map<String, Number> queryVector) {
            super(scoreScript, queryVector);
        }

        public double l1normSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(scoreScript._getIndexVersion(), value);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(scoreScript._getIndexVersion(), value);
            int queryIndex = 0;
            int docIndex = 0;
            double l1norm = 0;
            while (queryIndex < queryDims.length && docIndex < docDims.length) {
                if (queryDims[queryIndex] == docDims[docIndex]) {
                    l1norm += Math.abs(queryValues[queryIndex] - docValues[docIndex]);
                    queryIndex++;
                    docIndex++;
                } else if (queryDims[queryIndex] > docDims[docIndex]) {
                    l1norm += Math.abs(docValues[docIndex]); // 0 for missing query dim
                    docIndex++;
                } else {
                    l1norm += Math.abs(queryValues[queryIndex]); // 0 for missing doc dim
                    queryIndex++;
                }
            }
            while (queryIndex < queryDims.length) {
                l1norm += Math.abs(queryValues[queryIndex]); // 0 for missing doc dim
                queryIndex++;
            }
            while (docIndex < docDims.length) {
                l1norm += Math.abs(docValues[docIndex]); // 0 for missing query dim
                docIndex++;
            }
            return l1norm;
        }
    }

    // Calculate l2 norm (Euclidean distance) between a query's sparse vector and documents' sparse vectors
    public static final class L2NormSparse extends VectorSparseFunctions {
        public L2NormSparse(ScoreScript scoreScript, Map<String, Number> queryVector) {
           super(scoreScript, queryVector);
        }

        public double l2normSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(scoreScript._getIndexVersion(), value);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(scoreScript._getIndexVersion(), value);
            int queryIndex = 0;
            int docIndex = 0;
            double l2norm = 0;
            while (queryIndex < queryDims.length && docIndex < docDims.length) {
                if (queryDims[queryIndex] == docDims[docIndex]) {
                    double diff = queryValues[queryIndex] - docValues[docIndex];
                    l2norm += diff * diff;
                    queryIndex++;
                    docIndex++;
                } else if (queryDims[queryIndex] > docDims[docIndex]) {
                    double diff = docValues[docIndex]; // 0 for missing query dim
                    l2norm += diff * diff;
                    docIndex++;
                } else {
                    double diff = queryValues[queryIndex]; // 0 for missing doc dim
                    l2norm += diff * diff;
                    queryIndex++;
                }
            }
            while (queryIndex < queryDims.length) {
                l2norm += queryValues[queryIndex] * queryValues[queryIndex]; // 0 for missing doc dims
                queryIndex++;
            }
            while (docIndex < docDims.length) {
                l2norm += docValues[docIndex]* docValues[docIndex]; // 0 for missing query dims
                docIndex++;
            }
            return Math.sqrt(l2norm);
        }
    }

    // Calculate a dot product between a query's sparse vector and documents' sparse vectors
    public static final class DotProductSparse extends VectorSparseFunctions {
        public DotProductSparse(ScoreScript scoreScript, Map<String, Number> queryVector) {
           super(scoreScript, queryVector);
        }

        public double dotProductSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(scoreScript._getIndexVersion(), value);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(scoreScript._getIndexVersion(), value);
            return intDotProductSparse(queryValues, queryDims, docValues, docDims);
        }
    }

    // Calculate cosine similarity between a query's sparse vector and documents' sparse vectors
    public static final class CosineSimilaritySparse extends VectorSparseFunctions {
        final double queryVectorMagnitude;

        public CosineSimilaritySparse(ScoreScript scoreScript, Map<String, Number> queryVector) {
            super(scoreScript, queryVector);
            double dotProduct = 0;
            for (int i = 0; i< queryDims.length; i++) {
                dotProduct +=  queryValues[i] *  queryValues[i];
            }
            this.queryVectorMagnitude = Math.sqrt(dotProduct);
        }

        public double cosineSimilaritySparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(scoreScript._getIndexVersion(), value);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(scoreScript._getIndexVersion(), value);

            double docQueryDotProduct = intDotProductSparse(queryValues, queryDims, docValues, docDims);
            double docVectorMagnitude = 0.0f;
            if (scoreScript._getIndexVersion().onOrAfter(Version.V_7_4_0)) {
                docVectorMagnitude = VectorEncoderDecoder.decodeVectorMagnitude(scoreScript._getIndexVersion(), value);
            } else {
                for (float docValue : docValues) {
                    docVectorMagnitude += docValue * docValue;
                }
                docVectorMagnitude = (float) Math.sqrt(docVectorMagnitude);
            }

            return docQueryDotProduct / (docVectorMagnitude * queryVectorMagnitude);
        }
    }

    private static double intDotProductSparse(float[] v1Values, int[] v1Dims, float[] v2Values, int[] v2Dims) {
        double v1v2DotProduct = 0;
        int v1Index = 0;
        int v2Index = 0;
        // find common dimensions among vectors v1 and v2 and calculate dotProduct based on common dimensions
        while (v1Index < v1Values.length && v2Index < v2Values.length) {
            if (v1Dims[v1Index] == v2Dims[v2Index]) {
                v1v2DotProduct += v1Values[v1Index] * v2Values[v2Index];
                v1Index++;
                v2Index++;
            } else if (v1Dims[v1Index] > v2Dims[v2Index]) {
                v2Index++;
            } else {
                v1Index++;
            }
        }
        return v1v2DotProduct;
    }
}
