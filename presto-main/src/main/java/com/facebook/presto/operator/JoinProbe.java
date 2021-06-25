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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.util.List;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public final class JoinProbe
{
    public static final class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final int[] probeJoinChannels;
        private final int probeHashChannel; // a value of -1 means "not present"

        public JoinProbeFactory(int[] probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel)
        {
            this.probeOutputChannels = probeOutputChannels;
            this.probeJoinChannels = Ints.toArray(probeJoinChannels);
            this.probeHashChannel = probeHashChannel.orElse(-1);
        }

        public JoinProbe createJoinProbe(Page page)
        {
            return new JoinProbe(probeOutputChannels, page, probeJoinChannels, probeHashChannel >= 0 ? page.getBlock(probeHashChannel).getLoadedBlock() : null);
        }
    }

    private final int positionCount;
    private final int[] probeOutputChannels;
    private final Page page;
    private final Page probePage;
    @Nullable
    private final Block probeHashBlock;

    private final boolean probePageMayHaveNull;
    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, int[] probeJoinChannels, @Nullable Block probeHashBlock)
    {
        this.positionCount = page.getPositionCount();
        this.probeOutputChannels = probeOutputChannels;
        this.page = page;
        this.probePage = page.getLoadedPage(probeJoinChannels);
        this.probeHashBlock = probeHashBlock;
        this.probePageMayHaveNull = probePageMayHaveNull(probePage);
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        return ++position < positionCount;
    }

    public long getCurrentJoinPosition(LookupSource lookupSource)
    {
        if (probePageMayHaveNull && currentPositionContainsNull(probePage, position)) {
            return -1;
        }
        if (probeHashBlock != null) {
            long rawHash = BIGINT.getLong(probeHashBlock, position);
            return lookupSource.getJoinPosition(position, probePage, page, rawHash);
        }
        return lookupSource.getJoinPosition(position, probePage, page);
    }

    public int getPosition()
    {
        return position;
    }

    public Page getPage()
    {
        return page;
    }

    public int getEstimatedOutputSizeInBytesPerRow()
    {
        int sizeInBytes = 0;
        // Avoid division by zero when positionCount == 0
        if (positionCount > 0) {
            for (int channel : probeOutputChannels) {
                sizeInBytes += page.getBlock(channel).getSizeInBytes() / positionCount;
            }
        }
        return sizeInBytes;
    }

    private static boolean currentPositionContainsNull(Page probePage, int position)
    {
        for (int channel = 0; channel < probePage.getChannelCount(); channel++) {
            if (probePage.getBlock(channel).isNull(position)) {
                return true;
            }
        }
        return false;
    }

    private static boolean probePageMayHaveNull(Page probePage)
    {
        for (int channel = 0; channel < probePage.getChannelCount(); channel++) {
            if (probePage.getBlock(channel).mayHaveNull()) {
                return true;
            }
        }
        return false;
    }
}
