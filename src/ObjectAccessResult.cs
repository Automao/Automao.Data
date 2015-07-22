using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Zongsoft.Data;

namespace Automao.Data
{
	#region 委托
	public delegate string CreateSql(ref int tableIndex, ref int joinStartIndex, ref int valueIndex, out object[] values);
	public delegate IEnumerable<T> Execute<T>(string sql, object[] values);
	#endregion

	public class ObjectAccessResult
	{
		#region 字段
		private ClassInfo _classInfo;
		private CreateSql _createSql;
		#endregion

		#region 构造函数
		public ObjectAccessResult(CreateSql createSql)
		{
			_createSql = createSql;
		}
		#endregion

		#region 方法
		public string CreateSql(ref int tableIndex, ref int joinStartIndex, ref int valueIndex, out object[] values)
		{
			return _createSql(ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
		}
		#endregion
	}

	public class ObjectAccessResult<T> : ObjectAccessResult, IEnumerable<T>, IEnumerator<T>
	{
		#region 静态字段
		private static readonly object _lock = new object();
		#endregion

		#region 字段
		private IEnumerator _enumerator;
		private Execute<T> _getResult;
		#endregion

		#region 构造函数
		public ObjectAccessResult(CreateSql createSql, Execute<T> execute)
			: base(createSql)
		{
			_getResult = execute;
		}
		#endregion

		#region 属性
		public IEnumerator Enumerator
		{
			get
			{
				if(_enumerator == null)
				{
					lock(_lock)
					{
						if(_enumerator == null)
						{
							var tableIndex = 0;
							var joinStartIndex = 0;
							var valueIndex = 0;
							object[] values;
							var sql = base.CreateSql(ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
							_enumerator = _getResult(sql, values).GetEnumerator();
						}
					}
				}
				return _enumerator;
			}
		}
		#endregion

		#region IEnumberable<T>成员
		public IEnumerator<T> GetEnumerator()
		{
			return this;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.Enumerator;
		}
		#endregion

		#region IEnumerator<T> 成员
		public void Dispose()
		{
		}

		object IEnumerator.Current
		{
			get
			{
				return ((IEnumerable)this).GetEnumerator().Current;
			}
		}

		public bool MoveNext()
		{
			return this.Enumerator.MoveNext();
		}

		public void Reset()
		{
		}

		T IEnumerator<T>.Current
		{
			get
			{
				return (T)((IEnumerator)this).Current;
			}
		}
		#endregion
	}
}
