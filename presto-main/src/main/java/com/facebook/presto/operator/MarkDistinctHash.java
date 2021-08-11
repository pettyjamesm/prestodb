/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDictionaryAggregationEnabled;
import static com.facebook.presto.operator.GroupByHash.createGroupByHash;

public class MarkDistinctHash
{
    private final GroupByHash groupByHash;
    private long nextDistinctId;

    public MarkDistinctHash(Session session, List<Type> types, int[] channels, Optional<Integer> hashChannel, JoinCompiler joinCompiler, UpdateMemory updateMemory)
    {
        this(session, types, channels, hashChannel, 10_000, joinCompiler, updateMemory);
    }

    public MarkDistinctHash(Session session, List<Type> types, int[] channels, Optional<Integer> hashChannel, int expectedDistinctValues, JoinCompiler joinCompiler, UpdateMemory updateMemory)
    {
        this.groupByHash = createGroupByHash(types, channels, hashChannel, expectedDistinctValues, isDictionaryAggregationEnabled(session), joinCompiler, updateMemory);
    }

    public long getEstimatedSize()
    {
        return groupByHash.getEstimatedSize();
    }

    public Work<Block> markDistinctRows(Page page)
    {
        return new TransformWork<>(groupByHash.getGroupIds(page), this::processNextGroupByIdsBlock);
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupByHash.getCapacity();
    }

    private Block processNextGroupByIdsBlock(GroupByIdBlock ids)
    {
        if (ids.getGroupCount() == nextDistinctId) {
            // no new distinct values, return RLE block of false value
            return new RunLengthEncodedBlock(new ByteArrayBlock(1, Optional.empty(), new byte[]{0}), ids.getPositionCount());
        }
        byte[] distinctMask = new byte[ids.getPositionCount()];
        for (int i = 0; i < distinctMask.length; i++) {
            byte isDistinct = ids.getGroupId(i) == nextDistinctId ? (byte) 1 : (byte) 0;
            distinctMask[i] = isDistinct;
            nextDistinctId += isDistinct;
        }
        return new ByteArrayBlock(distinctMask.length, Optional.empty(), distinctMask);
    }
}
