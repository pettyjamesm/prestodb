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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.common.Page;

import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class PageChannelSelector
        implements Function<Page, Page>
{
    @Override
    public abstract Page apply(Page page);

    public abstract Page loadAndApply(Page page);

    public static PageChannelSelector create(int inputChannelCount, int... outputChannels)
    {
        boolean isIdentityChannelMap = true;
        for (int i = 0; i < outputChannels.length; i++) {
            int channel = outputChannels[i];
            checkArgument(channel >= 0 && channel < inputChannelCount, "channels must be within range [0, %s), found: %s", inputChannelCount, channel);
            isIdentityChannelMap &= channel == i;
        }
        if (isIdentityChannelMap && inputChannelCount == outputChannels.length) {
            return IdentityChannelsSelector.INSTANCE;
        }
        return new SpecificChannelsSelector(outputChannels);
    }

    public static PageChannelSelector identity()
    {
        return IdentityChannelsSelector.INSTANCE;
    }

    private static final class IdentityChannelsSelector
            extends PageChannelSelector
    {
        private static final IdentityChannelsSelector INSTANCE = new IdentityChannelsSelector();

        @Override
        public Page apply(Page page)
        {
            return requireNonNull(page, "page is null");
        }

        @Override
        public Page loadAndApply(Page page)
        {
            return requireNonNull(page, "page is null").getLoadedPage();
        }
    }

    private static final class SpecificChannelsSelector
            extends PageChannelSelector
    {
        private final int[] channels;

        public SpecificChannelsSelector(int... channels)
        {
            this.channels = requireNonNull(channels, "channels is null").clone();
            checkArgument(IntStream.of(channels).allMatch(channel -> channel >= 0), "channels must be positive");
        }

        @Override
        public Page apply(Page page)
        {
            return requireNonNull(page, "page is null").extractChannels(channels);
        }

        @Override
        public Page loadAndApply(Page page)
        {
            return requireNonNull(page, "page is null").getLoadedPage(channels);
        }
    }
}
