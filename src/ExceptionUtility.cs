/*
 * Authors:
 *   钟峰(Popeye Zhong) <9555843@qq.com>
 *
 * Copyright (C) 2015-2017 Automao Network Co., Ltd. <http://www.zongsoft.com>
 *
 * This file is part of Automao.Data.
 *
 * Automao.Data is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * Automao.Data is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Automao.Data; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

using System;
using System.Collections.Generic;

namespace Automao.Data
{
	internal static class ExceptionUtility
	{
		#region 常量定义
		private const string MYSQL = "MySQL";
		private const string RESOURCE_ERROR_PREFIX = "Message.ERROR_";

		private const int ERROR_CODE_DUPLICATEKEY = 1062;
		#endregion

		#region 公共方法
		public static Zongsoft.Data.DataException GetDataException(global::MySql.Data.MySqlClient.MySqlException innerException)
		{
			if(innerException == null)
				return null;

			string message;

			if(!Zongsoft.Resources.ResourceUtility.TryGetString(RESOURCE_ERROR_PREFIX + innerException.Number.ToString(), out message))
				message = innerException.Message;

			switch(innerException.Number)
			{
				case ERROR_CODE_DUPLICATEKEY:
					return new Zongsoft.Data.DataConflictException(MYSQL, innerException.Number, message, innerException);
			}

			return new Zongsoft.Data.DataAccessException(MYSQL, innerException.Number, innerException);
		}
		#endregion
	}
}
