using System;
using System.Collections;
using System.Collections.Generic;

namespace DatabaseWrapper
{
    /// <summary>
    /// A thread-safe list with support for:
    /// 1) negative indexes (read from end).  "myList[-1]" gets the last value
    /// 2) modification while enumerating: enumerates a copy of the collection.
    /// Taken from StackOverflow (see link in seealso).
    /// </summary>
    /// <seealso cref="http://stackoverflow.com/questions/9995266/how-to-create-a-thread-safe-generic-list"/>
    /// <typeparam name="TValue"></typeparam>
    internal class ConcurrentList<TValue> : IList<TValue>
    {
        private object _lock = new object();
        private List<TValue> _storage = new List<TValue>();
        /// <summary>
        /// support for negative indexes (read from end).  "myList[-1]" gets the last value
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public TValue this[int index]
        {
            get
            {
                lock (_lock)
                {
                    if (index < 0)
                    {
                        index = this.Count - index;
                    }
                    return _storage[index];
                }
            }
            set
            {
                lock (_lock)
                {
                    if (index < 0)
                    {
                        index = this.Count - index;
                    }
                    _storage[index] = value;
                }
            }
        }

        public void Sort()
        {
            lock (_lock)
            {
                _storage.Sort();
            }
        }

        public int Count
        {
            get
            {
                return _storage.Count;
            }
        }

        bool ICollection<TValue>.IsReadOnly
        {
            get
            {
                return ((IList<TValue>)_storage).IsReadOnly;
            }
        }

        public void Add(TValue item)
        {
            lock (_lock)
            {
                _storage.Add(item);
            }
        }

        public void Clear()
        {
            lock (_lock)
            {
                _storage.Clear();
            }
        }

        public bool Contains(TValue item)
        {
            lock (_lock)
            {
                return _storage.Contains(item);
            }
        }

        public void CopyTo(TValue[] array, int arrayIndex)
        {
            lock (_lock)
            {
                _storage.CopyTo(array, arrayIndex);
            }
        }

        public int IndexOf(TValue item)
        {
            lock (_lock)
            {
                return _storage.IndexOf(item);
            }
        }

        public void Insert(int index, TValue item)
        {
            lock (_lock)
            {
                _storage.Insert(index, item);
            }
        }

        public bool Remove(TValue item)
        {
            lock (_lock)
            {
                return _storage.Remove(item);
            }
        }

        public void RemoveAt(int index)
        {
            lock (_lock)
            {
                _storage.RemoveAt(index);
            }
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            lock (_lock)
            {
                // return (IEnumerator<TValue>)_storage.ToArray().GetEnumerator();
                return _storage.GetEnumerator();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}