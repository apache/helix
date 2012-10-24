/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.alerts;

public class Stat {
	String _name;
	Tuple<String> _value;
	Tuple<String> _timestamp;
	
	public Stat(String name, Tuple<String> value, Tuple<String> timestamp)
	{
		_name = name;
		_value = value;
		_timestamp = timestamp;
	}
	
	public String getName()
	{
		return _name;
	}
	
	public Tuple<String> getValue()
	{
		return _value;
	}
	
	public Tuple<String> getTimestamp()
	{
		return _timestamp;
	}
}
