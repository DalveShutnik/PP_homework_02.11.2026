#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <syncstream>
#include <thread>
#include <vector>

struct task_t {
  std::chrono::milliseconds time{200};
  void operator()();
};

struct th_pool_t {
  using f_t = std::function< void(void) >;
  explicit th_pool_t(unsigned int k, unsigned int batch = 1);
  ~th_pool_t();
  void submit(f_t f);
  void expand(unsigned int k);
  void shrink(unsigned int k);
  size_t size() const;
private:
  void execute(size_t id);
  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  bool stop_{false};
  std::vector< std::thread > ths_;
  std::vector< bool > quit_;
  unsigned int batch_;
};

void th_pool_t::submit(f_t f) {
  mx_.lock(); tasks_.push(std::move(f)); mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k, unsigned int batch):
  batch_(batch ? batch : 1)
{
  expand(k);
}

void th_pool_t::expand(unsigned int k) {
  std::lock_guard lck(mx_);
  while (ths_.size() < k) {
    size_t id = ths_.size();
    quit_.push_back(false);
    ths_.emplace_back([this, id]() { execute(id); });
  }
}

void th_pool_t::shrink(unsigned int k) {
  if (k == 0 || k >= ths_.size()) return;
  mx_.lock();
  for (size_t i = k; i < quit_.size(); ++i) quit_[i] = true;
  mx_.unlock();
  cd_.notify_all();
  for (size_t i = k; i < ths_.size(); ++i) ths_[i].join();
  mx_.lock();
  ths_.resize(k);
  quit_.resize(k);
  mx_.unlock();
}

size_t th_pool_t::size() const {
  return ths_.size();
}

th_pool_t::~th_pool_t() {
  mx_.lock(); stop_ = true; mx_.unlock();
  cd_.notify_all();
  for (auto && th: ths_) th.join();
}

void th_pool_t::execute(size_t id) {
  std::unique_lock lck(mx_);
  do {
    auto proceed = [&]() {
      return !tasks_.empty() || stop_ || quit_[id];
    };
    cd_.wait(lck, proceed);
    if (quit_[id]) break;
    if (tasks_.empty()) continue;
    std::vector< f_t > batch;
    while (!tasks_.empty() && batch.size() < batch_) {
      batch.push_back(std::move(tasks_.front()));
      tasks_.pop();
    }
    lck.unlock();
    for (auto && task: batch) {
      try { task(); } catch (...) {}
    }
    lck.lock();
  } while (!stop_ || !tasks_.empty());
}

void task_t::operator()() {
  std::this_thread::sleep_for(time);
  std::osyncstream(std::cout) << std::this_thread::get_id() << ": " << time.count() << " мс\n";
}

int main() {
  using namespace std::chrono_literals;
  {
    th_pool_t pool(2);
    std::osyncstream(std::cout) << "потоков: " << pool.size() << "\n";
    for (size_t i = 0; i < 10; ++i) pool.submit(task_t{});
    pool.expand(5);
    std::osyncstream(std::cout) << "потоков: " << pool.size() << "\n";
    for (size_t i = 0; i < 10; ++i) pool.submit(task_t{});
    pool.shrink(1);
    std::osyncstream(std::cout) << "потоков: " << pool.size() << "\n";
    for (size_t i = 0; i < 10; ++i) pool.submit(task_t{});
  }
  std::cout << "неравномерные задачи:\n";
  auto start = std::chrono::steady_clock::now();
  {
    th_pool_t pool(3, 4);
    for (size_t i = 0; i < 4; ++i) pool.submit(task_t{1000ms});
    for (size_t i = 0; i < 8; ++i) pool.submit(task_t{100ms});
  }
  auto time = std::chrono::steady_clock::now() - start;
  auto ms = std::chrono::duration_cast< std::chrono::milliseconds >(time).count();
  std::cout << "время: " << ms << " мс\n";
}
