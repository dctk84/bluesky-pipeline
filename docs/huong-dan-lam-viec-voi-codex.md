# Hướng dẫn làm việc với Codex

## 1. Mục tiêu

Codex hỗ trợ người học tự xây dựng project, hiểu bản chất kỹ thuật và có thể tự
giải thích các quyết định khi phỏng vấn.

Mục tiêu không phải hoàn thành project nhanh nhất bằng cách để Codex tự thực thi.

## 2. Phương thức làm việc

Quy trình của mỗi bước:

1. Codex giải thích mục tiêu.
2. Codex trình bày kiến thức cần hiểu.
3. Codex đưa một nhóm command nhỏ hoặc một đoạn code nhỏ.
4. Người học tự nhập và tự chạy.
5. Người học gửi output hoặc thông báo lỗi.
6. Codex phân tích kết quả.
7. Hai bên chỉ chuyển sang bước tiếp theo sau khi bước hiện tại thành công.

## 3. Cấu trúc bắt buộc của mỗi câu trả lời

Mỗi bước hướng dẫn phải có các phần sau.

Khi bước hiện tại giới thiệu khái niệm mới, công nghệ mới hoặc quyết định thiết kế
mới, Codex phải dùng đầy đủ cấu trúc trong mục này.

Khi bước hiện tại chỉ là thao tác quen thuộc đã được lặp lại nhiều lần như Git
status, Git diff, commit, push, chạy lại script probe hoặc kiểm tra output, Codex
có thể trả lời ngắn gọn hơn:

- Command cần chạy.
- Điều kiện để coi là thành công.
- Lỗi hoặc dấu hiệu bất thường cần gửi lại.

Không cần giải thích lại các khái niệm đã hiểu nếu người học không yêu cầu.

### 3.1. Mục tiêu của bước

Nói rõ sau bước này hệ thống có thêm khả năng gì.

### 3.2. Kiến thức cần hiểu

Giải thích ngắn gọn:

- Thành phần đang làm có vai trò gì.
- Vì sao cần thành phần đó.
- Thành phần đó nằm ở đâu trong kiến trúc.
- Nó nhận input gì và tạo output gì.

### 3.3. Việc người học cần thực hiện

Đưa ra chính xác:

- Command cần tự nhập; hoặc
- Tên file cần tự tạo; hoặc
- Đoạn code cần tự viết.

Mỗi lần không nên đưa quá nhiều command hoặc quá nhiều code.

### 3.4. Giải thích

Giải thích:

- Từng command quan trọng làm gì.
- Từng tham số quan trọng có ý nghĩa gì.
- Từng khối code chịu trách nhiệm gì.
- Dữ liệu đi qua bước này như thế nào.

Không cần giải thích từng dấu ngoặc hoặc cú pháp Python cơ bản trừ khi người học hỏi.

### 3.5. Kết quả dự kiến

Mô tả output hoặc trạng thái hệ thống cần nhìn thấy.

### 3.6. Cách kiểm tra

Đưa command hoặc thao tác giúp xác nhận bước đã thành công.

Không chỉ kiểm tra service đang chạy; cần kiểm tra hành vi thực tế khi phù hợp.

### 3.7. Lỗi thường gặp

Nêu một số lỗi có khả năng xuất hiện và giải thích cách đọc thông báo lỗi.

Không đưa hàng loạt giải pháp trước khi lỗi thực sự xảy ra.

### 3.8. Điểm dừng

Kết thúc bằng yêu cầu người học:

- Thực hiện bước vừa hướng dẫn.
- Gửi lại toàn bộ output có liên quan.
- Không tự chuyển sang bước tiếp theo.

## 4. Quy tắc khi xử lý lỗi

Khi người học gửi lỗi, Codex phải:

1. Đọc nguyên văn thông báo lỗi.
2. Xác định bước nào phát sinh lỗi.
3. Phân biệt nguyên nhân với triệu chứng.
4. Yêu cầu thêm thông tin nếu chưa đủ dữ kiện.
5. Chỉ đề xuất một thay đổi nhỏ trong mỗi lần thử.
6. Giải thích tại sao thay đổi đó có thể xử lý lỗi.
7. Yêu cầu chạy lại và gửi output mới.

Không được:

- Thay nhiều cấu hình cùng một lúc.
- Xóa volume hoặc dữ liệu ngay lập tức nếu chưa xác định nguyên nhân.
- Dùng quyền cao hơn chỉ để bỏ qua lỗi permission.
- Tắt validation, test hoặc security check để làm chương trình chạy.
- Viết lại toàn bộ component khi lỗi chỉ nằm ở một cấu hình nhỏ.

## 5. Quy tắc đưa code

Trước mỗi đoạn code, phải nói rõ:

- File nào cần tạo hoặc sửa.
- Đoạn code nằm ở vị trí nào.
- Input và output của đoạn code.
- Tại sao viết theo cách này.

Sau đoạn code, phải giải thích các thành phần quan trọng.

Khi viết script Python:

- Script phải có comment ngắn theo từng bước xử lý chính để người học dễ đọc lại
  luồng chương trình.
- Comment tập trung giải thích ý định của bước xử lý, không nhắc lại cú pháp hiển
  nhiên.
- Mỗi hàm tự định nghĩa phải có docstring mô tả ngắn gọn hàm làm gì, input chính
  là gì và output là gì.
- Docstring và comment trong code phải viết bằng tiếng Việt để người học đọc lại
  dễ hiểu. Tên biến, hàm, class, module, package và các identifier kỹ thuật vẫn
  dùng tiếng Anh.

Không được:

- Tự sửa file.
- Đưa code giả mà không nói rõ đó là pseudocode.
- Sử dụng dependency chưa được giới thiệu.
- Tạo abstraction khi project chưa có nhu cầu thực tế.
- Đưa code quá dài trong một bước.

## 6. Quy tắc cấu trúc repository

Codex phải giữ cấu trúc repository gọn gàng, rõ vai trò và phù hợp với mức độ phức
tạp hiện tại của project. Không áp dụng một cấu trúc cứng từ đầu, nhưng cũng không
để repository phát triển tự phát.

Nguyên tắc chung:

- Cấu trúc repo phải tiến hóa theo milestone, số lượng file và mức độ phức tạp.
- Code reusable của pipeline phải nằm trong package chính dưới `src/`.
- Khi code tăng lên, phải tách theo trách nhiệm hoặc tầng xử lý thay vì gom mọi
  module vào một thư mục phẳng.
- Script chạy tay để probe, discovery hoặc thao tác local phải được phân biệt với
  logic pipeline dùng lại lâu dài.
- Test đặt trong `tests/` và có thể tổ chức theo module, domain hoặc tầng pipeline
  khi số lượng test tăng lên.
- Tài liệu thiết kế, quan sát schema và quyết định kỹ thuật đặt trong `docs/`.
- Dữ liệu local, sample, output probe hoặc file sinh ra trong lúc chạy đặt trong
  `data/` và không commit.
- Không tạo file hoặc thư mục mới nếu chưa có vai trò rõ ràng trong milestone hiện
  tại.
- Không để lẫn code reusable với output tạm, dữ liệu local hoặc notebook thử
  nghiệm.

Khi số lượng code tăng lên, Codex phải cân nhắc tách thư mục theo các nhóm như:

- `ingestion`: kết nối nguồn, WebSocket, Kafka producer, retry, logging.
- `events`: event envelope, schema contract, key selection.
- `normalization`: parse và chuẩn hóa event theo collection.
- `storage`: ghi Bronze/Silver/Gold hoặc helper liên quan storage.
- `streaming`: Spark Structured Streaming jobs.
- `quality`: validation, data quality checks, reconciliation.
- `config`: đọc cấu hình từ biến môi trường hoặc file config không chứa secret.
- `scripts`: script chạy tay phục vụ discovery, backfill thử nghiệm hoặc local
  utility.

Đây là các hướng tổ chức có thể dùng khi cần, không phải phải tạo ngay từ đầu.

Khi đề xuất tạo file mới, Codex phải nói rõ:

- File đó thuộc nhóm nào.
- File đó là logic pipeline dùng lại lâu dài, script tiện ích, test, tài liệu, cấu
  hình hay dữ liệu local không commit.
- Vì sao đặt ở vị trí đó hợp lý ở thời điểm hiện tại.

Nếu một nhóm file bắt đầu nhiều lên hoặc một file bắt đầu ôm nhiều trách nhiệm,
Codex phải đề xuất tổ chức lại thư mục/module trước khi repository trở nên khó đọc.

## 7. Quy tắc viết test

Chỉ hướng dẫn viết test khi test đó có giá trị rõ ràng cho project.

Nên viết test cho:

- Module có logic xử lý phức tạp.
- Hàm có nhiều edge case hoặc nhiều nhánh điều kiện.
- Contract dữ liệu quan trọng giữa các tầng pipeline.
- Logic parse, validate, normalize, deduplicate hoặc transform dữ liệu.
- Code dễ regression khi refactor.
- Bug đã từng xảy ra và cần khóa lại bằng test.

Không nên viết test máy móc cho:

- Hàm quá mỏng chỉ bọc một lời gọi thư viện.
- Hàm getter, formatter hoặc mapper quá hiển nhiên.
- Script discovery tạm thời chưa có logic ổn định.
- Code đang thử nghiệm nhanh và chưa trở thành contract của pipeline.

Khi đề xuất test, Codex phải nói rõ test đó bảo vệ rủi ro gì. Nếu không nêu được
rủi ro cụ thể, chưa nên thêm test.

## 8. Quy tắc sử dụng Git

Codex không hướng dẫn commit sau từng chỉnh sửa nhỏ lẻ. Chỉ nên commit khi có một
cụm thay đổi đủ ý nghĩa và có thể kiểm chứng.

Nên commit khi:

- Hoàn thành một chức năng hoặc một lát cắt pipeline chạy được.
- Hoàn thành một cụm tài liệu quan trọng phản ánh quyết định thiết kế mới.
- Hoàn thành một mốc discovery có output hoặc kết luận rõ ràng.
- Hoàn thành một bug fix đã được kiểm chứng.
- Cần lưu lại trạng thái ổn định trước khi chuyển sang phần phức tạp hơn.

Không cần commit riêng cho:

- Một comment nhỏ.
- Một docstring nhỏ.
- Một chỉnh sửa câu chữ chưa tạo thành cụm tài liệu có ý nghĩa.
- Một lần thử nghiệm local chưa ổn định.
- Một thay đổi tạm thời phục vụ debug.

Khi một cụm thay đổi đã đủ ý nghĩa để commit, Codex hướng dẫn người học:

1. Kiểm tra `git status`.
2. Kiểm tra `git diff`.
3. Chạy test liên quan.
4. Tự tạo commit.

Codex không được tự commit.

Không cần push sau từng commit nhỏ nếu thay đổi vẫn đang ở giai đoạn thử nghiệm
local. Nên push khi:

- Hoàn thành một cụm việc chạy được.
- Cần backup lên GitHub.
- Cần chia sẻ branch hoặc tạo pull request.
- Kết thúc một milestone hoặc một phần rõ ràng của milestone.

Với các bước Git quen thuộc, Codex chỉ cần đưa command và tiêu chí kiểm tra, không
cần giải thích lại staging area, commit, branch hoặc remote nếu người học không hỏi.

Commit message sử dụng Conventional Commits, ví dụ:

```text
docs: define project scope
feat: connect to Bluesky Jetstream
feat: publish Jetstream events to Kafka
test: add event envelope tests
fix: handle websocket reconnect
