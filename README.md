ОТВЕТЫ НА ЗАМЕЧАНИЯ В ФАЙЛЕ замечания.txt в resources.

# praktikum_module_2
Итоговый проект второго модуля Apache Kafka (Яндекс Практикум)

Это стандартное java-приложение, для работы необходимо запустить метод main класса KafkaApplication.
Для запуска предпочтительней использовать IDE (например, IntelliJ IDEA).

До запуска приложения необходимо:
1. Запустить Docker.
2. Скопировать из папки resources файл docker-compose.yml на локальную машину 
и запустить его командой 'docker-compose up -d' через консоль.
3. Создать топики через консоль - команды можно скопировать из файла topic.txt, который лежит в resources.
4. Запустить KafkaApplication.

После запуска приложения класс Producer начнёт отправлять сообщения в топики messages и deprecated-words каждые 20 секунд.
Содержимое отправленных в топики сообщений будет логироваться.
Топик messages предназначен для обмена сообщениями между пользователями, в топике обрабатываются сообщения типа MyMessage.
В топике deprecated-words содержатся запрещённые слова, на которые наложена цензура.
В топике filtered-messages содержатся сообщения после осуществления фильтрации.

Класс KafkaTableHandler преобразует содержимое топика deprecated-words в персистентное хранилище deprecated-word-store,
которое далее будет использоваться для поиска запрещённых слов в сообщениях.

Класс KafkaStreamsMessageFilter реализует логику по фильтрации сообщений.
1. В методе filterUsers осуществляется фильтрация заблокированных пользователей.
Для примера user-1 не желает получать сообщения от user-6 и user-8 (список хранится в resources файл blocked_user1.txt).
В классе MyMessage есть поля author и recipient, на основе них мы можем отфильтровать нежелательные сообщения
2. В методе filterWords осуществляется подмена запрещённых слов на символ *.
В классе MyMessage есть поле message, в котором хранится текст сообщения.

Внутри самих классов по коду есть комментарии какая настройка за что отвечает.

Чтобы посмотреть отфильтрованные сообщения в консоле необходимо выполнить команду:
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9095, localhost:9096 --topic filtered-messages --max-messages 20

{"author":"user-1","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-7","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-0","message":"* you man!","recipient":"user-1"}
{"author":"user-3","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-4","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-9","message":"what a piece of *?","recipient":"user-1"}
{"author":"user-2","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-5","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-0","message":"* you man!","recipient":"user-1"}
{"author":"user-3","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-4","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-9","message":"what a piece of *?","recipient":"user-1"}
{"author":"user-2","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-5","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-1","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-7","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-1","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-7","message":"Hello, buddy!","recipient":"user-1"}
{"author":"user-0","message":"* you man!","recipient":"user-1"}
{"author":"user-3","message":"Hello, buddy!","recipient":"user-1"}
Processed a total of 20 messages

В консоле видим 20 обработанных сообщеений, сообщения с author user-6 или user-8 и получателем user-1 отсутствуют,
т.к. были отфильтрованы.
Также есть ряд сообщений "* you man!" и "what a piece of *?", в которых запрещённые слова были заменены на *.

Обновлённый результат работы приложения:
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9095, localhost:9096 --topic filtered-messages --max-messages 10
{"author":"user-0","message":"Hello World ","recipient":"user-1"}
{"author":"user-1","message":"Hello World ","recipient":"user-1"}
{"author":"user-2","message":"Hello World buddy what * ","recipient":"user-1"}
{"author":"user-5","message":"Hello World buddy what * piece man ","recipient":"user-1"}
{"author":"user-1","message":"Hello World buddy what * piece man ","recipient":"user-1"}
{"author":"user-0","message":"Hello World buddy what * ","recipient":"user-1"}
{"author":"user-3","message":"Hello ","recipient":"user-1"}
{"author":"user-4","message":"Hello ","recipient":"user-1"}
{"author":"user-1","message":"Hello World buddy what * ","recipient":"user-1"}
{"author":"user-7","message":"Hello World buddy ","recipient":"user-1"}
Processed a total of 10 messages
