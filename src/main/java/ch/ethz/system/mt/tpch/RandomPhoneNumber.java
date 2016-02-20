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
package ch.ethz.system.mt.tpch;

import java.util.Random;

import static java.util.Locale.ENGLISH;

public class RandomPhoneNumber
        extends AbstractRandomInt
{
    // limited by country codes in phone numbers
    private static final int NATIONS_MAX = 90;

    public RandomPhoneNumber(long seed)
    {
        this(seed, 1);
    }

    public RandomPhoneNumber(long seed, int expectedRowCount)
    {
        super(seed, 3 * expectedRowCount);
    }

    public String nextValue(long nationKey)
    {
        int format = new Random().nextInt(10) % 3;
        String number = new String("");
        switch (format) {
            case 0:
                number = String.format(ENGLISH,
                        "%02d-%03d-%03d-%04d",
                        (10 + (nationKey % NATIONS_MAX)),
                        nextInt(100, 999),
                        nextInt(100, 999),
                        nextInt(1000, 9999));
                break;
            case 1:
                number = String.format(ENGLISH,
                        "00%02d-%03d-%03d-%04d",
                        (10 + (nationKey % NATIONS_MAX)),
                        nextInt(100, 999),
                        nextInt(100, 999),
                        nextInt(1000, 9999));
                break;
            case 2:
                number = String.format(ENGLISH,
                        "+%02d-%03d-%03d-%04d",
                        (10 + (nationKey % NATIONS_MAX)),
                        nextInt(100, 999),
                        nextInt(100, 999),
                        nextInt(1000, 9999));
                break;
        }
        return number;
    }
}
