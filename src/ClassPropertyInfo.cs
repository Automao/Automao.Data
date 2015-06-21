using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Automao.Data
{

	/// <summary>
	/// 属性详情
	/// </summary>
	public class ClassPropertyInfo
	{
		public ClassInfo Host
		{
			get;
			set;
		}

		/// <summary>
		/// 类中属性名称
		/// </summary>
		public string ClassPropertyName
		{
			get;
			set;
		}
		/// <summary>
		/// 表中列名
		/// </summary>
		public string TableColumnName
		{
			get;
			set;
		}
		/// <summary>
		/// 数据类型
		/// </summary>
		public string DbType
		{
			get;
			set;
		}
		/// <summary>
		/// 数据大小
		/// </summary>
		public int? Size
		{
			get;
			set;
		}
		/// <summary>
		/// 是否为主键
		/// </summary>
		public bool IsPKColumn
		{
			get;
			set;
		}

		/// <summary>
		/// 是否传入构造函数
		/// </summary>
		public bool PassedIntoConstructor
		{
			get;
			set;
		}
		/// <summary>
		/// 构造函数对应参数名称
		/// </summary>
		public string ConstructorName
		{
			get;
			set;
		}
		/// <summary>
		/// 是否为输出参数
		/// </summary>
		public bool IsOutPutParamer
		{
			get;
			set;
		}
		/// <summary>
		/// 是否为外键
		/// </summary>
		public bool IsFKColumn
		{
			get;
			set;
		}
		/// <summary>
		/// 是否可为空
		/// </summary>
		public bool Nullable
		{
			get;
			set;
		}
		/// <summary>
		/// 外键关联主表
		/// </summary>
		public ClassInfo Join
		{
			get;
			set;
		}
		/// <summary>
		/// 外键关联列
		/// </summary>
		public ClassPropertyInfo JoinColumn
		{
			get;
			set;
		}
		/// <summary>
		/// 要将关联对像赋值给当前名称指定的属性
		/// </summary>
		public string SetClassPropertyName
		{
			get;
			set;
		}
	}
}
