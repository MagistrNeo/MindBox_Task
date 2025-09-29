import math
from abc import ABC, abstractmethod
from typing import Union

class Shape(ABC):
    """Абстрактный базовый класс для всех фигур"""
    
    @abstractmethod
    def area(self) -> float:
        """Вычисляет площадь фигуры"""
        pass
    
    @abstractmethod
    def is_right_angled(self) -> bool:
        """Проверяет, является ли фигура прямоугольной (если применимо)"""
        pass

class Circle(Shape):
    """Класс для работы с кругом"""
    
    def __init__(self, radius: float):
        if radius <= 0:
            raise ValueError("Радиус должен быть положительным числом")
        self.radius = radius
    
    def area(self) -> float:
        """Вычисляет площадь круга по радиусу"""
        return math.pi * self.radius ** 2
    
    def is_right_angled(self) -> bool:
        """Круг не может быть прямоугольным"""
        return False

class Triangle(Shape):
    """Класс для работы с треугольником"""
    
    def __init__(self, a: float, b: float, c: float):
        # Проверка на положительные стороны
        if a <= 0 or b <= 0 or c <= 0:
            raise ValueError("Все стороны треугольника должны быть положительными числами")
        
        # Проверка неравенства треугольника
        if a + b <= c or a + c <= b or b + c <= a:
            raise ValueError("С заданными сторонами невозможно построить треугольник")
        
        self.a = a
        self.b = b
        self.c = c
    
    def area(self) -> float:
        """Вычисляет площадь треугольника по трем сторонам (формула Герона)"""
        s = (self.a + self.b + self.c) / 2
        return math.sqrt(s * (s - self.a) * (s - self.b) * (s - self.c))
    
    def is_right_angled(self, tolerance: float = 1e-7) -> bool:
        """
        Проверяет, является ли треугольник прямоугольным
        с учетом погрешности вычислений с плавающей точкой
        """
        sides = sorted([self.a, self.b, self.c])
        # Теорема Пифагора: a² + b² = c²
        return abs(sides[0]**2 + sides[1]**2 - sides[2]**2) < tolerance

class Rectangle(Shape):
    """Пример добавления новой фигуры - прямоугольник"""
    
    def __init__(self, width: float, height: float):
        if width <= 0 or height <= 0:
            raise ValueError("Ширина и высота должны быть положительными числами")
        self.width = width
        self.height = height
    
    def area(self) -> float:
        """Вычисляет площадь прямоугольника"""
        return self.width * self.height
    
    def is_right_angled(self) -> bool:
        """Прямоугольник всегда прямоугольный"""
        return True

def calculate_area(shape: Shape) -> float:
    """
    Вычисляет площадь фигуры без знания ее типа в compile-time
    
    Args:
        shape: Объект фигуры, наследуемый от Shape
    
    Returns:
        float: Площадь фигуры
    """
    return shape.area()

def is_right_angled_triangle(a: float, b: float, c: float, tolerance: float = 1e-7) -> bool:
    """
    Утилитарная функция для проверки, является ли треугольник прямоугольным
    по трем сторонам без создания объекта Triangle
    
    Args:
        a, b, c: Стороны треугольника
        tolerance: Допустимая погрешность
    
    Returns:
        bool: True если треугольник прямоугольный
    """
    sides = sorted([a, b, c])

    return abs(sides[0]**2 + sides[1]**2 - sides[2]**2) < tolerance
