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
	public delegate CreateSqlResult CreateSql(CreatingSqlParameter parameter);
	public delegate IEnumerable<T> Execute<T>(CreateSqlResult result);
	#endregion

	public class ObjectAccessResult
	{
		#region 字段
		private CreateSql _createSql;
		#endregion

		#region 构造函数
		public ObjectAccessResult(CreateSql createSql)
		{
			_createSql = createSql;
		}
		#endregion

		#region 方法
		public CreateSqlResult CreateSql(CreatingSqlParameter parameter)
		{
			return _createSql(parameter);
		}
		#endregion
	}

	public class ObjectAccessResult<T> : ObjectAccessResult, IEnumerator<T>, ICollection, IList<T>, IList
	{
		#region 静态字段
		private static readonly object _lock = new object();
		#endregion

		#region 字段
		private IEnumerator _enumerator;
		private Execute<T> _getResult;
		private List<T> _list;
		#endregion

		#region 构造函数
		public ObjectAccessResult(CreateSql createSql, Execute<T> execute) : base(createSql)
		{
			_getResult = execute;
		}
		#endregion

		#region 属性
		private List<T> List
		{
			get
			{
				if(_list == null)
				{
					lock(_lock)
					{
						if(_list == null)
						{
							var parameter = new CreatingSqlParameter(false, 0, 0, 0);
							var result = base.CreateSql(parameter);
							_list = _getResult(result).ToList();
						}
					}
				}
				return _list;
			}
		}

		public IEnumerator Enumerator
		{
			get
			{
				if(_enumerator == null)
					_enumerator = List.GetEnumerator();
				return _enumerator;
			}
		}
		#endregion

		#region IEnumberable<T>成员
		public IEnumerator<T> GetEnumerator()
		{
			if(_enumerator != null)
				_enumerator.Reset();

			return this;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			if(_enumerator != null)
				_enumerator.Reset();

			return this;
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
				return _enumerator.Current;
			}
		}

		public bool MoveNext()
		{
			var enumerator = _enumerator ?? this.Enumerator;
			return enumerator.MoveNext();
		}

		public void Reset()
		{
			_enumerator.Reset();
		}

		T IEnumerator<T>.Current
		{
			get
			{
				return (T)((IEnumerator)this).Current;
			}
		}
		#endregion

		#region ICollection 成员
		public void CopyTo(Array array, int index)
		{
			((ICollection)List).CopyTo(array, index);
		}

		public int Count
		{
			get
			{
				return List.Count;
			}
		}

		public bool IsSynchronized
		{
			get
			{
				return ((ICollection)List).IsSynchronized;
			}
		}

		public object SyncRoot
		{
			get
			{
				return ((ICollection)List).SyncRoot;
			}
		}
		#endregion

		#region IList<T> 成员
		public int IndexOf(T item)
		{
			return List.IndexOf(item);
		}

		public void Insert(int index, T item)
		{
			List.Insert(index, item);
		}

		public void RemoveAt(int index)
		{
			List.RemoveAt(index);
		}

		public T this[int index]
		{
			get
			{
				return List[index];
			}
			set
			{
				List[index] = value;
			}
		}
		#endregion

		#region ICollection<T> 成员
		public void Add(T item)
		{
			List.Add(item);
		}

		public void Clear()
		{
			List.Clear();
		}

		public bool Contains(T item)
		{
			return List.Contains(item);
		}

		public void CopyTo(T[] array, int arrayIndex)
		{
			((ICollection<T>)List).CopyTo(array, arrayIndex);
		}

		public bool IsReadOnly
		{
			get
			{
				return ((ICollection<T>)List).IsReadOnly;
			}
		}

		public bool Remove(T item)
		{
			return List.Remove(item);
		}
		#endregion

		#region IList 成员
		public int Add(object value)
		{
			return ((IList)List).Add(value);
		}

		public bool Contains(object value)
		{
			return ((IList)List).Contains(value);
		}

		public int IndexOf(object value)
		{
			return ((IList)List).IndexOf(value);
		}

		public void Insert(int index, object value)
		{
			((IList)List).Insert(index, value);
		}

		public bool IsFixedSize
		{
			get
			{
				return ((IList)List).IsFixedSize;
			}
		}

		public void Remove(object value)
		{
			((IList)List).Remove(value);
		}

		object IList.this[int index]
		{
			get
			{
				return ((IList)List)[index];
			}
			set
			{
				((IList)List)[index] = value;
			}
		}
		#endregion
	}
}
