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

public enum CustomerColumn
        implements TpchColumn<Customer>
{
    CUSTOMER_KEY("custkey", TpchColumnType.BIGINT)
            {
                public long getLong(Customer customer)
                {
                    return customer.getCustomerKey();
                }
            },

    NAME("name", TpchColumnType.VARCHAR)
            {
                public String getString(Customer customer)
                {
                    return customer.getName();
                }
            },

    ADDRESS("address", TpchColumnType.VARCHAR)
            {
                public String getString(Customer customer)
                {
                    return customer.getAddress();
                }
            },

    NATION_KEY("nationkey", TpchColumnType.BIGINT)
            {
                public long getLong(Customer customer)
                {
                    return customer.getNationKey();
                }
            },

    PHONE("phone", TpchColumnType.VARCHAR)
            {
                public String getString(Customer customer)
                {
                    return customer.getPhone();
                }
            },

    ACCOUNT_BALANCE("acctbal", TpchColumnType.DOUBLE)
            {
                public double getDouble(Customer customer)
                {
                    return customer.getAccountBalance();
                }

                public long getLong(Customer customer)
                {
                    return customer.getAccountBalanceInCents();
                }
            },

    MARKET_SEGMENT("mktsegment", TpchColumnType.VARCHAR)
            {
                public String getString(Customer customer)
                {
                    return customer.getMarketSegment();
                }
            },

    COMMENT("comment", TpchColumnType.VARCHAR)
            {
                public String getString(Customer customer)
                {
                    return customer.getComment();
                }
            };

    private final String columnName;
    private final TpchColumnType type;

    CustomerColumn(String columnName, TpchColumnType type)
    {
        this.columnName = columnName;
        this.type = type;
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public TpchColumnType getType()
    {
        return type;
    }

    @Override
    public double getDouble(Customer customer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Customer customer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(Customer customer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(Customer entity)
    {
        throw new UnsupportedOperationException();
    }
}
